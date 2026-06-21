# Spark Core — 持久化与 Checkpoint 实战

## 什么时候需要持久化？

在 Spark 中，每个 `Transformation` 操作都是惰性的（`lazy`），只有当遇到 `Action` 时才会真正触发计算。默认情况下，同一个 RDD 每次调用 `Action` 都会从头开始重新计算整个血缘链。如果你的 DAG 中有一个"中间结果"被后续多个操作复用，不做缓存就意味着重复计算——这在数据量大、计算链长的时候会带来巨大的性能浪费。

> **面试点**：Spark 为什么要设计成 `lazy`？回答要点——便于优化执行计划（`Catalyst Optimizer` / `Tungsten`），能合并算子、减少 Shuffle，避免不必要的中间物化。

### 误区：缓存一切

很多初学者刚接触 Spark 时有一个常见误区——**不管三七二十一，先把每个 RDD 都 `.cache()` 再说**。这种"缓存一切"的做法不仅不能提升性能，反而会浪费宝贵的内存资源，甚至导致 `OOM`（`Out Of Memory`）。

```scala
// ❌ 错误：只用一次的 RDD 不需要缓存
val rdd = sc.textFile("hdfs://data").cache()
rdd.count()  // 只用一次，缓存浪费

// ✅ 正确：只用一次的不缓存
val rdd = sc.textFile("hdfs://data")
rdd.count()
```

**为什么会浪费？** 因为 `count()` 是一个 `Action`，执行完后缓存在内存中的数据再也不会被用到。而 `MEMORY_ONLY` 存储的数据占用的是 `Storage Memory` 区域，这部分内存本来可以用来缓存其他真正需要复用的中间结果。

> **踩坑经验**：曾经有同学在生产环境里对所有 `DataFrame` 都调了 `.cache()`，5 个 10GB 的中间结果同时驻留内存，直接把 `spark.executor.memory` 撑爆，`GC` 频繁到 `Task` 一直 `Lost`。排查了半天才发现问题——**只缓存确实复用的数据**。

### 哪些场景必须缓存

缓存的核心原则是：**一个 RDD 被多个 `Action` 复用**，或者**迭代计算中反复用到同一份数据**。

```scala
// 场景 1：迭代计算（同一数据被多次使用）
val baseRDD = sc.textFile("hdfs://data").filter(_.contains("ERROR")).cache()
val hourCount = baseRDD.map(extractHour).countByValue()
val typeCount = baseRDD.map(extractType).countByValue()
val levelCount = baseRDD.map(extractLevel).countByValue()

// 场景 2：ML 训练（机器学习迭代）
var model = initialModel
val data = prepareData().cache()
for (i <- 1 to 100) {
  model = train(data, model)  // 每次迭代都用到 data
}

// 场景 3：DAG 中的分叉点
val stage1 = loadData()
  .transform1().cache()  // ← 分叉点，后续两个分支都依赖它
val output1 = stage1.transform2().action()
val output2 = stage1.transform3().action()
```

**场景 1 分析**：`baseRDD` 被三个 `countByValue()` 操作复用，如果不缓存，每个 `countByValue()` 都会从 `sc.textFile()` 开始重跑整个血缘——包括读取 HDFS、`filter` 等全部步骤。

**场景 2 分析**：机器学习训练中，`data` 在每一轮迭代（epoch）中都被用到。200 轮迭代意味着 `prepareData()` 会被执行 200 次——做了缓存之后只执行 1 次，这节省的时间是数量级上的差距。

**场景 3 分析**：DAG 分叉点的典型场景——同一个基础数据经过不同变换后产出不同的结果。如果不缓存 `stage1`，两个 `Action` 会使 `loadData()` 和 `transform1()` 各执行两次。

```scala
// 验证缓存是否生效
scala> rdd.getStorageLevel
// res: org.apache.spark.storage.StorageLevel = StorageLevel(1 replicas)

// 查看缓存在 UI 上的占用
// Spark Web UI → Storage 页签

// 手动清理
rdd.unpersist()
```

`unpersist()` 是立即生效的，它会标记对应的 `Block` 为可删除。如果不手动调用，当 `SparkContext` 停止时或通过 `LRU` 淘汰时也会自动清理。

### 小结：哪些场景该缓存？

| 场景 | 示例 | 缓存建议 |
|------|------|----------|
| 单次使用的 RDD | `rdd.count()` | 不缓存 |
| 多次复用的中间结果 | 报表中的多维度聚合 | 必须缓存 |
| 迭代计算 | ML 训练、PageRank | 必须缓存 |
| DAG 分叉点 | 多分支 Transform | 必须缓存 |
| Shuffle 后的中间结果 | `groupByKey` 之后的 RDD | 按需缓存 |

## 缓存策略选择

Spark 提供了丰富的缓存策略（`StorageLevel`），不同的策略在**内存占用**、**CPU 开销**、**容错能力**之间做不同的权衡。选择正确的策略，往往能在不增加硬件成本的情况下让作业性能翻倍。

### 内存足够时的选择

当你的数据量显著小于 Executor 可用内存时，选择很简单——追求最极致的读取速度。

```scala
// 方案 1：MEMORY_ONLY（默认，最快）
rdd.persist(StorageLevel.MEMORY_ONLY)
// 数据完全反序列化存放在堆内存
// CPU 省（不用反序列化），内存占用大
// 适合：数据量 < 可用内存
```

`MEMORY_ONLY` 是 `cache()` 和 `persist()` 不带参数时的默认策略。数据以 Java 对象的原生形态存于堆内存中，读取时零反序列化开销，是**所有策略中读取最快的**。但 Java 对象头（`object header`）的开销很大——一个 `Integer` 对象在堆上占用 16 字节，而它实际只存了 4 字节的 `int` 值。这就是为什么 `MEMORY_ONLY` 的"内存膨胀率"常常达到 2-3 倍。

```scala
// 方案 2：MEMORY_ONLY_SER（内存有限时）
import org.apache.spark.storage.StorageLevel
rdd.persist(StorageLevel.MEMORY_ONLY_SER)
// 数据序列化后存放，内存占用约 1/3~1/5
// CPU 多（每次读取要反序列化）
// 适合：内存紧但可以接受额外 CPU 开销
```

`MEMORY_ONLY_SER` 会把数据以字节数组（`byte[]`）的形式存放，默认使用 Java 序列化，也可以配置 `Kryo` 序列化器（`spark.serializer`）来取得更好的压缩比。`Kryo` 序列化后的数据大小通常是 Java 序列化的 **1/10**，这对于大数据场景是巨大的内存节省。

> **踩坑经验**：使用 `MEMORY_ONLY_SER` 时记得注册 `Kryo` 类，否则 Kryo 仍然会写入全类名，压缩效果大打折扣：`conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")` 配合 `conf.registerKryoClasses(Array(classOf[YourType]))`。

### 内存不足时的选择

当数据量超过可用内存时，你就需要在**速度**和**可靠性**之间做权衡了。

```scala
// 方案 3：MEMORY_AND_DISK（安全方案）
rdd.persist(StorageLevel.MEMORY_AND_DISK)
// 先放内存，放不下再溢写到磁盘
// 读取时：优先从内存读，没有则从磁盘读
// 适合：数据量略大于内存
```

`MEMORY_AND_DISK` 是最推荐的生产环境默认选择。它的策略是**能放内存的放内存，多出来的溢写到磁盘**。读取时先从内存找，找不到再从磁盘读。这样即使内存不足，作业也不会挂掉——代价只是溢写部分的读取速度变慢（从纳秒级的内存访问降为毫秒级的磁盘 I/O）。

```scala
// 方案 4：DISK_ONLY（最慢但最安全）
rdd.persist(StorageLevel.DISK_ONLY)
// 全部写磁盘
// 适合：数据太大内存放不下，但比重新计算快
```

什么时候用 `DISK_ONLY`？如果你的中间结果是**计算代价极大**（比如几十次 `join` 之后的结果）但数据本身就比可用内存大很多，这时候 `DISK_ONLY` 比每次从头重算要快得多。

> **面试点**：为什么不直接用 `MEMORY_AND_DISK` 代替 `DISK_ONLY`？因为 `MEMORY_AND_DISK` 仍然会尝试往内存放，如果内存并不够，反复的 `GC` 和 `eviction`（逐出）反而会降低整体性能。有些场景下直接全量写到磁盘反而更稳定。

### 缓存策略对比总表

| 策略 | 存储位置 | 是否序列化 | 读速度 | 内存占用 | CPU 开销 | 容错 |
|------|----------|-----------|--------|---------|---------|------|
| `MEMORY_ONLY` | 内存 | 否（反序列化） | ⭐⭐⭐⭐⭐ | 高 | 低 | 低 |
| `MEMORY_ONLY_SER` | 内存 | 是 | ⭐⭐⭐⭐ | 低 | 中 | 低 |
| `MEMORY_AND_DISK` | 内存+磁盘 | 否 | ⭐⭐⭐ | 中 | 低 | 中 |
| `MEMORY_AND_DISK_SER` | 内存+磁盘 | 是 | ⭐⭐ | 低 | 中 | 中 |
| `DISK_ONLY` | 磁盘 | 是 | ⭐ | 无 | 中 | 中 |

### 序列化对比测试

下面通过一段实际代码，直观展示不同的 `StorageLevel` 在内存占用上的差异。

```scala
// 通过 repartition 构造相同数据量进行比较
val data = sc.parallelize(1 to 10000000)
  .map(i => (i % 1000, "value_" * 100))

// MEMORY_ONLY — 存入内存
data.persist(StorageLevel.MEMORY_ONLY).count()

// MEMORY_ONLY_SER — 序列化后存入内存
data.persist(StorageLevel.MEMORY_ONLY_SER).count()

// 在 Spark UI Storage 页看到两次的占用差异
// MEMORY_ONLY: ~1.2GB（反序列化对象头开销大）
// MEMORY_ONLY_SER: ~200MB（序列化后紧凑存储）
```

结果很惊人——同样的数据量，`MEMORY_ONLY` 占用了近 1.2GB，而 `MEMORY_ONLY_SER` 只有 200MB 左右，差了整整 **6 倍**。原因很简单：Java 对象在堆内存中有对象头（`mark word` + `klass pointer`，64 位 JVM 下通常 12-16 字节），加上 `String` 内部的 `char[]` 引用、`Tuple2` 的包装等等，对象结构膨胀非常严重。而序列化后这些都是连续的字节数组，没有额外的对象开销。

## 缓存的数据结构

### BlockManager

Spark 的缓存系统核心是 **BlockManager**，每个 Executor 上都有一个 BlockManager 实例。它相当于 Spark 的"分布式内存文件系统"——负责数据的写入、读取、跨节点传输。

```
Executor 上的 BlockManager：
┌──────────────────────────────────────────────┐
│  BlockManager                                │
│  ┌──────────┐  ┌──────────┐  ┌────────────┐ │
│  │ MemoryStore│  │ DiskStore │  │ BlockTransfer│ │
│  │ (内存)    │  │ (磁盘)   │  │ Service    │ │
│  └──────────┘  └──────────┘  └────────────┘ │
│                                              │
│  ┌──── Block ────┐                          │
│  │ BlockId: rdd_2_5 │                        │
│  │ Level: MEMORY_AND_DISK                   │
│  │ Size: 28.5 MB                            │
│  └────────────────┘                          │
└──────────────────────────────────────────────┘
```

`BlockManager` 由三个核心组件构成：

- **`MemoryStore`**：管理内存中的 `Block`。当内存不够时，根据 `StorageLevel` 决定是否溢写到磁盘或直接淘汰。
- **`DiskStore`**：管理磁盘上的 `Block`。写入路径通常是 `spark.local.dir` 指定的目录。
- **`BlockTransferService`**：负责跨 Executor 的数据传输。当需要从远端拉取 `Block` 时（比如 `Shuffle`），由这个组件负责网络 I/O。

每个 `Block` 都有一个唯一的 `BlockId`，格式为 `rdd_<rddId>_<partitionIndex>`。例如 `rdd_2_5` 表示 RDD ID 为 2、分区索引为 5 的缓存块。

> **面试点**：`BlockManager` 和 `ShuffleManager` 的关系？`ShuffleManager` 负责 Shuffle Write/Read 阶段的磁盘文件管理，而 `BlockManager` 负责缓存数据的存储和管理。在 Shuffle Read 时，`BlockManager` 的 `BlockTransferService` 会被用来从远程 Executor 拉取 Shuffle 数据。

### Block 的生命周期

1. **写入**：`Action` 触发的计算完成后，每个分区的计算结果以 `Block` 为单位写入 `BlockManager`。
2. **读取**：后续 `Action` 如果依赖同一个 RDD，先根据 `BlockId` 在本地 `BlockManager` 中查找，找不到则通过 `BlockManagerMaster`（Driver 端）查询元数据，确认数据在哪台 Executor 上，然后通过 `BlockTransferService` 远程拉取。
3. **淘汰**：当 `Storage Memory` 达到上限时，`MemoryStore` 按照 `LRU`（`Least Recently Used`）策略淘汰 `Block`。被淘汰的 `Block` 根据 `StorageLevel` 决定是丢弃还是溢写到磁盘。
4. **删除**：调用 `unpersist()` 或在 `SparkContext` 停止时，`BlockManager` 清理所有关联的 `Block`。

## Checkpoint 实战

### 何时使用 Checkpoint

`cache` 和 `persist` 虽然好用，但它们有一个根本缺陷——**不切断血缘（`Lineage`）**。这意味着如果节点故障导致缓存数据丢失，Spark 仍然可以通过血缘关系重新计算丢失的分区。但如果血缘链本身非常长（几十到上百个 `Transformation`），重新计算的代价可能比直接保存落盘数据还要高。

这就是 `Checkpoint` 的用武之地——**把中间结果持久化到可靠存储（通常是 HDFS）并切断血缘关系**。

```
Checkpoint 的必要条件：
1. DAG 非常深（几十甚至上百个 Transformation）
2. 反复使用同一路径的中继数据
3. 需要屏蔽子 RDD 故障影响父 RDD 的场景

Checkpoint 的代价：
- 写 HDFS 会有 3 副本 → 磁盘 ×3
- 写操作本身较慢（HDFS 写入）
```

**典型使用场景**：在某大厂的实时数仓场景中，一个 ETL 作业从 Kafka 消费数据，经过 30+ 步的 `join`、`groupBy`、`window` 操作后产出最终结果。DAG 图在 Spark UI 上密密麻麻，血缘链长得看不到尽头。这种情况下，每隔几个关键节点做一次 `Checkpoint`，可以大幅降低故障恢复的时间。

```scala
// Checkpoint 的基本使用
spark.sparkContext.setCheckpointDir("hdfs://namenode:8020/checkpoint/etl-job-001")

val rawData = spark.sparkContext.textFile("hdfs://data/input")
val stage1 = rawData.filter(_.contains("ERROR")).map(parseLog)
val stage2 = stage1.groupBy(_.hour).mapValues(_.size)

stage2.checkpoint()  // 在关键节点做 checkpoint
stage2.count()       // Action 触发 checkpoint 写入

// 此时 stage2 的血缘被切断
// stage2 的依赖从原始的 stage1 → rawData 变为直接依赖 HDFS 上的 checkpoint 文件
```

### 与本地 Checkpoint 的区别

Spark 2.1 之后引入了更轻量的 `localCheckpoint`，它不写入 HDFS，而是写入 Executor 的本地磁盘。

```scala
// 本地 Checkpoint — Spark 2.1+ 新增
// 不写入 HDFS，写入 Executor 本地磁盘
// 切断血缘但无副本，丢失后无法恢复
spark.sparkContext.setCheckpointDir("/tmp/checkpoint")
rdd.localCheckpoint()  // 更轻量的 checkpoint
```

`localCheckpoint` 的优点和缺点都很突出：

| 特性 | `checkpoint` | `localCheckpoint` |
|------|-------------|-------------------|
| 存储位置 | HDFS（分布式） | Executor 本地磁盘 |
| 副本数 | 3（HDFS 默认） | 1（无副本） |
| 写速度 | 慢（网络 I/O + 副本复制） | 快（本地磁盘 I/O） |
| 容错性 | 高（节点故障不影响） | 低（节点故障数据丢失） |
| 适用场景 | 重要中间结果 | DAG 过长需要截断血缘 |
| 带宽占用 | 消耗网络带宽 | 不消耗网络带宽 |

```scala
// 三种"持久化"机制对比

// 1. 普通缓存 — 最快但不安全
rdd.persist(StorageLevel.MEMORY_AND_DISK)

// 2. 本地 Checkpoint — 轻量级血缘截断
rdd.localCheckpoint()

// 3. 完整 Checkpoint — 写在 HDFS，可靠
rdd.checkpoint()
```

> **踩坑经验**：`localCheckpoint` 在 `Spark Streaming` 的 `stateful` 操作中非常有用。如果一个 `DStream` 的 `transform` 链特别长，定期调用 `localCheckpoint()` 可以防止 `Driver` 端的 `Lineage` 元数据无限膨胀导致 `OOM`。

### Checkpoint 的工作原理

当你调用了 `rdd.checkpoint()` 时，Spark 实际上做了三件事：

1. **标记**：在 RDD 上打一个「需要 checkpoint」的标记，但此时尚未开始写入。
2. **触发**：当第一个 `Action`（比如 `count()`）执行时，Job 完成后会启动一个额外的作业，专门用于将 RDD 的数据写入 `checkpointDir`。
3. **截断血缘**：写入完成后，RDD 的 `dependencies` 被清空，`parent RDD` 引用被移除，RDD 的 `compute` 函数被替换为直接读取 HDFS 上的 checkpoint 文件。

这个过程意味着——**checkpoint 会额外触发一次 Job**。如果你的数据是巨量的，这个额外开销不可忽略。

### checkpoint 和 cache 配合使用的最佳实践

```scala
// ✅ 最佳实践：先 cache 再 checkpoint
val processed = data
  .map(complexTransform)
  .persist(StorageLevel.MEMORY_AND_DISK)

processed.checkpoint()
processed.count()  // count 时：先写入内存缓存，再写入 HDFS checkpoint
```

先 `persist()` 后 `checkpoint()` 的原因很简单——checkpoint 的 Job 会重新读取 RDD 的数据来写入 HDFS。如果已经 `persist` 过了，checkpoint 读取的就是缓存中的快照数据，避免了额外的全量计算。如果只 `checkpoint` 而不 `persist`，checkpoint 的 Job 会从头开始重新计算整个血缘链——这完全违背了 checkpoint 的初衷。

> **面试点**：`cache()` 和 `checkpoint()` 的顺序为什么不能反过来？因为 checkpoint 的写入是在独立的 Job 中完成的，它需要读取 RDD 的 `compute` 函数来获取数据。如果在 checkpoint 之前没有 cache，compute 函数会从头执行所有 Transformation。先 cache 再 checkpoint，checkpoint 就可以利用 cache 的数据。

## 缓存调优

### 缓存数据逐出

当 `Storage Memory` 满了，`BlockManager` 需要决定哪些缓存数据可以被清理——这个过程称为 `eviction`（逐出）。

```scala
// 当 Storage Memory 满了，BlockManager 会：
// 1. 根据 StorageLevel 决定是否逐出
// 2. MEMORY_ONLY → 整个 Block 被逐出（丢失缓存）
// 3. MEMORY_AND_DISK → 数据写到磁盘（保留在 Storage 中）
// 4. 被逐出的 Block 下次使用时需要重新计算
```

**逐出策略的细节**：
- `MEMORY_ONLY` 的 Block 被逐出后缓存丢失，后续使用时从血缘重新计算。
- `MEMORY_AND_DISK` 的 Block 被逐出时会先尝试写入磁盘（`DiskStore`），在 `Storage UI` 上显示为 `"dropped from memory, written to disk"`。
- `DISK_ONLY` 的 Block 不受 `Storage Memory` 影响，一直存在磁盘上。

**什么情况下会触发逐出？** 当 `Storage Memory` 的使用率超过 `spark.storage.storageFraction`（默认 0.5，即 Unified Memory 的 50% 部分）时，新写入的 Block 会触发 `eviction`。

### 缓存复用技巧

```scala
// 技巧 1：缓存不同的序列化形式
val cached = data.cache()
cached.count()        // 触发缓存（MEMORY_ONLY）

// 技巧 2：配合 checkpoint 使用
val processed = data
  .map(complexTransform)
  .persist(StorageLevel.MEMORY_AND_DISK)

processed.checkpoint()  // 双重保障
processed.count()       // 写入缓存 + 写入 checkpoint
```

**技巧 1** 说明：`cache()` 本质是 `persist(StorageLevel.MEMORY_ONLY)` 的简写。对于较小的数据集，直接使用默认策略简单直接。

**技巧 2** 说明：在实际生产中，这是最推荐的"保险组合"。即使用户误操作导致缓存被清理，`checkpoint` 还能兜底。反之，如果节点没有故障，缓存提供快速读取，`checkpoint` 文件也写好了，以后 `Job` 重启也能直接使用。

```scala
// 技巧 3：只缓存必要列（DataFrame/DataSet）
// 不要缓存全量宽表，只缓存需要的列
val slimDF = wideDF.select("id", "category", "amount").cache()
slimDF.count()
// 相比缓存宽表，内存占用可以降低 50%-80%

// 技巧 4：适当增加 Storage Fraction
// 默认 spark.memory.storageFraction = 0.5
// 如果缓存数据多，可以调高到 0.6-0.7
// spark.conf.set("spark.memory.storageFraction", "0.7")
```

### 查看缓存状态

除了通过 `Spark Web UI` 的 `Storage` 页签查看缓存占用，还可以通过编程方式主动监控。

```scala
// 编程方式查看
sc.getPersistentRDDs.foreach { case (id, rdd) =>
  println(s"RDD $id: ${rdd.getStorageLevel}, ${rdd.count} partitions")
}
```

**Web UI 上的关键指标**：
- **Storage Level**：当前使用的缓存策略
- **Size in Memory**：内存中的缓存数据大小
- **Size on Disk**：磁盘上的缓存数据大小（仅 `MEMORY_AND_DISK` / `DISK_ONLY`）
- **Cached Partitions**：已缓存的分区数（如果少于总分区数，说明部分分区被逐出了）
- **Replication**：副本数

> **踩坑经验**：如果 `Storage` 页显示 `Cached Partitions` 长期少于总分区数，说明缓存放不下全部数据，需要检查 `StorageLevel` 是否合理，或者调整 `spark.executor.memory` 和 `spark.memory.storageFraction`。

## 面试高频考点

### Q: cache 后的 RDD 如果内存不够会怎么样？

如果是 `MEMORY_ONLY`，放不下的分区会被逐出，下次使用时重新计算。如果是 `MEMORY_AND_DISK`，放不下的会写入磁盘。

**深入追问**：那如果是 `MEMORY_ONLY_SER` 呢？同样会被逐出，差别只是序列化后的数据更紧凑，能存下更多分区，减少了逐出的概率。但逐出时的处理机制和 `MEMORY_ONLY` 是一样的。

### Q: MEMORY_ONLY_SER 有什么缺点？

`MEMORY_ONLY_SER` 序列化存储省内存，但每次读取都需要反序列化，增加 CPU 开销。适合 CPU 充裕、内存紧张的场景。

**补充说明**：反序列化的代价不只是 CPU 时间，还会产生大量的临时对象，增加 `GC` 压力。如果你的 `Executor` 已经因为 `GC` 频繁导致 `Task` 执行缓慢，用 `MEMORY_ONLY_SER` 可能会雪上加霜。这时候更推荐 `MEMORY_AND_DISK` 方案。

### Q: checkpoint 和 cache 能不能一起用？

可以，而且这是推荐做法。先 `cache()` 再 `checkpoint()`，checkpoint 写 HDFS 时会复用缓存的数据，避免重新计算。注意顺序：先 `persist()` 再 `checkpoint()`，最后 `count()` 触发计算。

**延伸思考**：如果先 `checkpoint` 再 `cache` 会怎样？`checkpoint` 的 Job 需要从头计算一遍数据并写入 HDFS，然后再 `cache` 又要计算一遍（因为 checkpoint 没有把数据留在内存），**数据被计算了两遍**，正好踩了"重复计算"的坑。

### Q: localCheckpoint 和 checkpoint 的区别？

`localCheckpoint` 写本地磁盘（不复制），速度快但故障不可恢复。`checkpoint` 写 HDFS（3 副本），速度慢但可靠。localCheckpoint 适合 DAG 特别长需要截断血缘、但数据不是特别重要的场景。

**实战建议**：在开发测试环境中，优先使用 `localCheckpoint` 来验证逻辑正确性。上线前再评估是否需要改为完整的 `checkpoint`。这样可以节省 HDFS 存储空间，同时不影响开发效率。

### Q: persist 和 cache 的区别？

`cache()` 是 `persist(StorageLevel.MEMORY_ONLY)` 的简写。两者在功能上完全等价，`cache()` 更简洁，`persist()` 可以指定更细粒度的 `StorageLevel`。

**变形问题**：`df.cache().count` 和 `df.createOrReplaceTempView("t").sparkSession.sql("CACHE TABLE t")` 有什么区别？后者可以指定 `LAZY` 关键字，延迟到第一次查询时才真正缓存。前者通过 `count()` 立即触发缓存。

## 小结

| 策略 | 场景 | 速率 | 可靠性 |
|------|------|------|--------|
| MEMORY_ONLY | 数据小于内存 | 最快 | 低 |
| MEMORY_AND_DISK | 数据略大于内存 | 较快 | 中 |
| MEMORY_ONLY_SER | 内存紧张 | 快 | 低 |
| DISK_ONLY | 数据超大 | 慢 | 中 |
| Checkpoint(HDFS) | 需要容错 | 慢 | 高 |
| localCheckpoint | 截断血缘 | 较快 | 低 |

**一句话总结**：缓存是**性能优化**手段，checkpoint 是**容错优化**手段。两者不冲突，配合使用效果最佳。生产环境的通用做法是——`MEMORY_AND_DISK` 作为默认缓存策略 + 关键节点的 `checkpoint` 做双重保障。尤其是在 DAG 深度较大、计算代价高的 ETL 任务中，这一组合能显著提升作业的稳定性和恢复速度。
