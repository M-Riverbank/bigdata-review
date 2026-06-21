# Spark Core — 内存管理与持久化

## Spark 内存管理架构

### 为什么需要内存管理？

在大数据场景下，Spark 应用需要处理海量数据，Shuffle、Join、Aggregation 等操作都会在内存中产生大量的中间数据。如果没有一套高效的内存管理机制，Spark 会面临几个严重问题：

- **OOM 频繁**：不管是缓存 RDD 还是执行 Shuffle，内存不够就直接崩溃
- **GC 风暴**：JVM 堆内充斥着大量小对象，Full GC 频繁触发，作业原地"卡死"
- **资源争抢**：Storage（缓存）和 Execution（计算）互相抢占内存，谁都不让谁

Spark 的内存管理就是为解决这些问题而生的。它的核心目标是：**在有限的 JVM 堆内，最大化内存利用率，同时保证计算任务不因内存不足而失败**。

### 两个阶段：从静态到统一

Spark 的内存管理经历了两个阶段：

| 阶段 | 版本 | 管理器 | 核心特点 | 问题 |
|------|------|--------|---------|------|
| 静态管理 | 1.6 之前 | `StaticMemoryManager` | Storage / Execution 各占固定比例，不能互相借用 | 内存利用率低，容易一边不够一边浪费 |
| 统一管理 | 1.6+ 至今 | `UnifiedMemoryManager` | Storage / Execution 动态借用，提高内存利用率 | 相对完善，但仍有踩坑空间 |

```
静态管理时代：
  Storage 内存 = 固定比例（如 50%）
  Execution 内存 = 固定比例（如 50%）
  └── Storage 满了但 Execution 空闲 → 浪费
  └── Execution 需要更多但 Storage 占着 → 只能溢写磁盘

统一管理时代：
  Storage 和 Execution 是"一个池子"里的水
  └── Execution 可以借用 Storage 的内存
  └── 整体利用率大幅提升
```

> **面试点**：面试官常问"统一内存管理相比静态管理解决了什么问题？"核心答案是两个字——**借用**。静态管理下 Storage 内存即使没人用，Execution 也不能碰；统一管理打破了这堵墙。

### 踩坑经验

在实际生产环境中，很多同学以为 Spark 1.6+ 之后就是"统一管理"了，不需要操心。但事实上，如果 `spark.memory.fraction` 和 `spark.memory.storageFraction` 配得不对，统一管理也救不了你。后文会详细讲怎么配。

## 统一内存管理详解

### 内存布局

理解 Spark 统一内存管理，首先要搞清楚 Executor JVM 堆内存的完整划分。很多初学者以为 `spark.executor.memory = 4G` 就意味着 Spark 能用 4GB 做任何事情，这是**大错特错**的。

下面以 `spark.executor.memory = 4G` 为例，逐层拆解：

```scala
// Executor JVM 内存完整划分（spark.executor.memory = 4G）

第一层：扣除 Reserved Memory
  Reserved Memory = 300MB（硬编码，不可配置）
  └── 用于 Spark 内部引擎对象创建、元数据存储
  └── 这 300MB 是"保护内存"，任何业务数据都不能占用

  剩余可分配内存 = 4096 - 300 = 3796MB

第二层：按 spark.memory.fraction 切分

  Spark Memory（Spark 引擎可用内存）= 3796 × 0.6 ≈ 2277MB
  │   └── 缓存 RDD/DataFrame、Broadcast 变量等
  │   └── Shuffle / Join / Sort / Aggregation 中间结果
  │
  User Memory（用户自定义数据）= 3796 × 0.4 ≈ 1519MB
      └── UDF 中创建的对象
      └── 用户自定义的集合、Map 等数据结构
      └── 与 Spark 内存隔离，互不影响

第三层：Spark Memory 内再按 storageFraction 切分

  Storage Memory（存储内存）= 2277 × 0.5 ≈ 1138MB
  │   └── RDD cache / persist
  │   └── Broadcast 变量
  │   └── accumulator 数据
  │
  Execution Memory（执行内存）= 2277 × 0.5 ≈ 1138MB
      └── Shuffle 读写缓冲区
      └── Join / Sort 的中间数据
      └── Aggregation 的 HashTable
```

**关键认识**：4GB Executor 堆，Spark 实际能用于计算+缓存的只有约 2.2GB，另外近 1.5GB 被 User Memory 占用，300MB 被 Reserved 占用。

> **面试点**：面试官可能问你"spark.executor.memory = 8G，spark.memory.fraction = 0.6，问 Spark Memory 和 User Memory 各是多少？"套公式：Spark Memory = (8G - 300MB) × 0.6 ≈ 4.6GB，User Memory ≈ 3.1GB。

### 动态借用机制

统一内存管理最核心的设计是**动态借用**。它规定了 Storage 和 Execution 之间如何"借"和"还"。

```
Execution 向 Storage 借用内存（允许）：
  Step 1: Execution 需要更多内存 → 检查 Storage 是否有空闲
  Step 2: Storage 有空闲 → 直接借用，无需归还
  Step 3: Storage 无空闲 → Execution 强制逐出 Storage 的缓存数据
  Step 4: 被逐出的缓存块 → 下次使用需重新计算（或从磁盘读回）

  ✅ 为什么允许？Execution 正在计算，不能失败（OOM 会丢任务进度）

Storage 向 Execution 借用内存（不允许）：
  ❌ 为什么不允许？Execution 的内存正在被计算任务使用
  ❌ 如果逐出 Execution 数据 → 计算中断 → 任务失败
```

这是一个**单向借用**的设计。Execution 有"最高优先级"——它可以随时把 Storage 的数据踢出去给自己腾地方。Storage 则只能被动等待。

```scala
// 动态借用机制的代码体现（UnifiedMemoryManager 核心逻辑简化版）

// Execution 申请内存时
def acquireExecutionMemory(numBytes: Long): Long = {
  // 1. 先从自己的 Execution 池拿
  val acquired = executionPool.acquire(numBytes)
  if (acquired >= numBytes) return acquired

  // 2. 不够 → 尝试从 Storage 借用
  val borrowed = storagePool.freeSpace(numBytes - acquired)
  // 3. Storage 释放空间给 Execution
  executionPool.increasePool(borrowed)
  acquired + borrowed
}

// Storage 申请内存时
def acquireStorageMemory(numBytes: Long): Long = {
  // Storage 只能用自己的份额，不能借用 Execution 的
  storagePool.acquire(numBytes)
}
```

> **面试点**：问"Execution 借用 Storage 内存后，Storage 的数据去哪了？"答案是：如果被逐出的数据有 `MEMORY_AND_DISK` 级别，会被写入磁盘；如果是 `MEMORY_ONLY`，则直接丢弃，下次使用时重新根据血缘计算。

### 踩坑经验：动态借用的副作用

动态借用看起来很完美，但实际生产中有一个**常见坑**：当 Storage 缓存大量数据且 Execution 频繁借用时，缓存数据会被频繁逐出。这导致两个问题：

1. **缓存震荡**：RDD 被反复 cache → evict → recompute → cache，CPU 和 IO 做无用功
2. **性能抖动**：作业运行时间不稳定，同一份代码每次跑的耗时差异很大

**解决方案**：对于核心的、确定会被多次复用的数据，可以考虑用 `MEMORY_AND_DISK` 或 `DISK_ONLY` 级别持久化，避免被 Execution 逐出后还要重算。

### 内存配置参数

下面这张表总结了内存相关的核心配置参数，建议**背下来**，面试和工作都用得上：

| 参数 | 默认值 | 说明 | 调优建议 |
|------|--------|------|---------|
| `spark.memory.fraction` | 0.6 | Spark 可用内存占总堆（扣除 Reserved）的比例 | 如果 UDF 大量用内存，适当降低；否则保持默认 |
| `spark.memory.storageFraction` | 0.5 | Storage 占 Spark Memory 的初始比例 | 缓存密集型作业可增大到 0.6-0.7 |
| `spark.memory.offHeap.enabled` | false | 是否启用堆外内存 | 大内存场景建议开启 |
| `spark.memory.offHeap.size` | 0 | 堆外内存大小（需同时开启 offHeap.enabled） | 通常设为 executor.memory 的 20-30% |
| `spark.storage.memoryMapThreshold` | 2MB | 磁盘映射读取的阈值 | 一般保持默认 |

> **面试点**：经常有面试官问"spark.memory.storageFraction = 0.5 是不是意味着 Storage 最多用 50%？"**不是**。这个 0.5 是初始比例。当 Execution 空闲时，Storage 可以借用 Execution 的内存来缓存更多数据。反过来，Execution 需要时也可以把 Storage 的数据踢走。

## 堆外内存（Off-Heap）

### 为什么需要堆外内存？

很多 Spark 初学者听说过"堆外内存"，但不太清楚它解决的是什么问题。我们从 JVM 堆内存的三大痛点说起：

```
JVM 堆内存的三大问题：

1. GC 暂停 — 堆越大，Full GC 暂停时间越长
   └── 4GB 堆的 Full GC 可能暂停数秒到数十秒
   └── 对于实时性要求高的作业，这是不可接受的

2. 内存碎片 — 大量小对象导致 GC 效率下降
   └── RDD 的每个 partition 在堆内都是独立对象
   └── 频繁创建和释放 → 堆内存碎片化 → GC 越来越慢

3. 序列化开销 — 数据在堆内需要 Java 对象表示
   └── 每条数据在 JVM 中都是一个对象（有对象头、指针等）
   └── 一个整数 4 字节，在 JVM 堆内可能占用 16-24 字节
```

Tungsten 项目（Spark 2.0+ 引入）提出了一个革命性的解决方案——**堆外内存**。它的核心思想是：**直接操作二进制数据，绕过 JVM 对象模型**。

```
堆外内存的优势：

1. 无 GC 影响 — 堆外内存不参与 JVM GC
   └── 即使是上百 GB 的堆外数据，也不会触发 Full GC

2. 紧凑存储 — 使用二进制格式，没有 Java 对象头开销
   └── 整数占 4 字节就是 4 字节，不会有额外开销

3. 零拷贝 — 数据可以直接通过网络发送（NIO DirectBuffer）
   └── 避免堆内 → 堆外的复制开销
```

```scala
// 启用堆外内存（需要在 SparkConf 中设置）
val spark = SparkSession.builder()
  .appName("OffHeapDemo")
  .config("spark.memory.offHeap.enabled", "true")
  .config("spark.memory.offHeap.size", "2g")  // 2GB 堆外内存
  .getOrCreate()

// 堆外内存的主要使用场景：
// 1. Tungsten 排序 — 排序过程中直接操作二进制数据，避免反序列化
// 2. Tungsten Shuffle — Shuffle 写/读阶段使用堆外缓冲区
// 3. 序列化后的数据存储 — 缓存序列化后的 RDD/DataFrame

// 注意：非序列化存储（如 MEMORY_ONLY）仍然使用堆内内存
// 只有 StorageLevel 带 _SER 或 OFF_HEAP 时才会用到堆外

import org.apache.spark.storage.StorageLevel

// 使用堆外缓存 RDD
rdd.persist(StorageLevel.OFF_HEAP)
// 等同于将数据存储在堆外，不占用 JVM 堆内存
```

### 堆外内存的局限性

堆外内存并不是银弹，它有以下几个需要注意的地方：

| 方面 | 说明 |
|------|------|
| 内存管理 | 需要手动管理，没有 JVM GC 帮你回收 |
| 序列化开销 | 数据必须序列化后才能放入堆外，读写有 CPU 开销 |
| 调试困难 | OOM 时堆外内存的排查比堆内困难得多 |
| 配置要求 | 需要操作系统支持，且受 `-XX:MaxDirectMemorySize` 限制 |

> **面试点**：面试官可能问"堆外内存和堆内内存谁更快？"答案是：**分场景**。堆外内存避免了 GC，但序列化/反序列化有 CPU 开销。对于大块连续数据的批量操作（如排序、Shuffle），堆外更快；对于频繁随机访问的小数据，堆内更快。

## RDD 持久化

### 为什么需要持久化？

这是 Spark 新手最容易忽视的问题。RDD 的默认行为是**重新计算（Recompute）**，每次调用 action 操作时，都会从头执行整个 lineage 链条。

```scala
// 一个经典的"重复计算"陷阱
val rdd1 = sc.textFile("hdfs://data/access_log_2024/")
val rdd2 = rdd1.filter(line => line.contains("ERROR"))
val rdd3 = rdd2.map(line => parseLog(line))

// 第一次 action — 从头计算：读取 HDFS → filter → map
println(s"ERROR 总数：${rdd3.count()}")

// 第二次 action — 又从头计算了一遍！！！
rdd3.saveAsTextFile("hdfs://output/errors/")

// 第三次 action — 再次从头计算！！！
println(s"不同错误类型数：${rdd3.map(_.errorType).distinct().count()}")
```

这个例子里，`rdd3.count()`、`rdd3.saveAsTextFile()`、`rdd3.distinct().count()` 三次 action，每次都从 HDFS 读取开始完整计算一遍。如果原始数据有 100GB，filter 后还剩 10GB，那三次 action 总共读取了 300GB 数据！

**为什么要设计成重复计算？**

```
RDD 的设计哲学：
  ✅ 容错：任何时候某个 partition 丢失 → 根据血缘重新计算即可
  ✅ 内存友好：计算完后就释放，不占用内存
  ❌ 性能问题：反复使用同一份数据时，重复计算浪费巨大

解决方案 → 持久化（persist / cache）
```

### persist / cache

持久化就是把 RDD 的计算结果保存下来，下次使用时直接从缓存读取，不再重复计算。

```scala
// cache 的底层实现
rdd.cache()
// 等价于：
rdd.persist(StorageLevel.MEMORY_ONLY)

// 所以以下两行完全等价：
rdd.persist()
rdd.cache()

// 释放缓存
rdd.unpersist()
```

```scala
// 改造上面的例子 — 加上持久化
val rdd1 = sc.textFile("hdfs://data/access_log_2024/")
val rdd2 = rdd1.filter(line => line.contains("ERROR"))
val rdd3 = rdd2.map(line => parseLog(line))

// 关键：持久化 rdd3
rdd3.persist(StorageLevel.MEMORY_AND_DISK)

// 第一次 action — 计算并缓存
println(s"ERROR 总数：${rdd3.count()}")

// 第二次 action — 直接读缓存，不用重算！
rdd3.saveAsTextFile("hdfs://output/errors/")

// 第三次 action — 还是读缓存！
println(s"不同错误类型数：${rdd3.map(_.errorType).distinct().count()}")
```

> **面试点**：面试官可能会问"cache 和 persist 有什么区别？"这是一个经典陷阱题。`cache()` 内部调用就是 `persist(StorageLevel.MEMORY_ONLY)`，二者本质没有区别。区别只在于 `persist()` 允许你传参指定存储级别。

### 存储级别

Spark 提供了多种存储级别，每一种都在"空间"和"速度"之间做了不同的权衡：

| 级别 | 空间 | CPU | 内存 | 磁盘 | 序列化 | 说明 |
|------|------|-----|------|------|--------|------|
| `MEMORY_ONLY` | 高 | 低 | 全内存 | 无 | 否 | 默认，速度最快 |
| `MEMORY_AND_DISK` | 高 | 中 | 部分 | 溢出 | 否 | 安全，内存不够写磁盘 |
| `MEMORY_ONLY_SER` | 中 | 中 | 序列化 | 无 | 是 | 省内存，但多一次反序列化开销 |
| `MEMORY_AND_DISK_SER` | 中 | 中 | 部分 | 溢出 | 是 | 省内存+安全，最均衡 |
| `DISK_ONLY` | 低 | 高 | 无 | 全磁盘 | 是 | 最慢，适合用一次的大数据 |
| `OFF_HEAP` | 中 | 中 | 堆外 | 无 | 是 | 无 GC，Tungsten 推荐 |

**如何选择？**

```
推荐优先级：
  1. MEMORY_ONLY — 数据量不大时首选
  2. MEMORY_ONLY_SER — 内存紧张但 CPU 充足
  3. MEMORY_AND_DISK — 不确定数据量大小，求稳
  4. MEMORY_AND_DISK_SER — 内存紧张+求稳
  5. DISK_ONLY — 数据超大且只用 1-2 次
  6. OFF_HEAP — 超大集群，需要精确控制 GC
```

### 使用场景

```scala
// ========== 场景 1：多次迭代计算 ==========
// 数据被多次 filter/map，每次都从源头开始太浪费
val baseRDD = sc.textFile("hdfs://data/2024-06-01/").cache()

val errorCount = baseRDD.filter(_.contains("ERROR")).count()
val warnCount = baseRDD.filter(_.contains("WARN")).count()
val infoCount = baseRDD.filter(_.contains("INFO")).count()
val debugCount = baseRDD.filter(_.contains("DEBUG")).count()
// baseRDD 被用了 4 次，不 cache 的话每次都要重新读 HDFS

// ========== 场景 2：ML 迭代训练 ==========
// 机器学习算法的核心：反复迭代计算同一个数据集
var weights = initialWeights
val dataRDD = sc.textFile("hdfs://data/train_set/").cache()

for (i <- 1 to 100) {
  // 每次迭代都基于同一个 dataRDD
  // 如果 dataRDD 没有 cache，每次循环都要重新从 HDFS 读取
  val gradient = dataRDD.map { record =>
    computeGradient(record, weights)
  }.reduce(_ + _)

  weights = updateWeights(weights, gradient)
}
// 不 cache 的话，100 次迭代 = 100 次全量 HDFS 读取
// cache 之后 = 1 次 HDFS 读取 + 99 次内存读取

// ========== 场景 3：Checkpoint 辅助 ==========
// 复杂 DAG，血缘链太长
val rdd = sc.textFile("hdfs://data/huge_dataset/")
  .flatMap(_.split("\\s+"))
  .map(word => (word, 1))
  .reduceByKey(_ + _)
  .filter(_._2 > 1000)
  .map(transform1)
  .map(transform2)
  .cache()  // 在 checkpoint 前 cache，避免 checkpoint 重复计算

rdd.checkpoint()
rdd.count()
```

### 踩坑经验：什么时候不该 cache？

cache 并不是万能的，以下场景 cache 反而有害：

```
1. 只使用一次的数据 → cache 浪费内存，还占用 Storage 空间
2. 数据量远大于可用内存 → 频繁 evict 导致"缓存震荡"
3. 数据源读取成本很低 → 比如本地文件，不如直接重算
4. 血统很短的 RDD → cache 收益微乎其微
```

## Checkpoint

### 为什么需要 Checkpoint？

RDD 的血缘（Lineage）设计带来了强大的容错能力——只要血缘在，任何时候 partition 丢失都能重算。但如果血缘链太长，这个"优势"反而成了负担：

```
RDD 血缘链太长的问题：

1. 恢复成本高 — 丢失一个 partition 要从源头开始重算
   └── 几十个 transformation 之后，重算代价堪比"重跑整个作业"

2. 依赖图占用 Driver 内存
   └── 每个 RDD 都保存了指向父 RDD 的引用
   └── 复杂 DAG 中，RDD 依赖链可能达到几十层
   └── Driver 内存被 lineage 数据占满 → Driver OOM

3. 调试困难
   └── 执行计划太复杂，定位问题像在迷宫里找路
```

**Checkpoint 的解决方案**就是：把中间结果存到可靠存储（HDFS），然后斩断血缘，让 RDD 直接依赖 checkpoint 数据。

```
Checkpoint 执行流程：

  第一阶段：标记（Driver 端）
    rdd.checkpoint()
    └── 只是打标记，不触发计算，不写入数据

  第二阶段：计算并写入（第一次 action 触发）
    rdd.count() / rdd.saveAsTextFile(...) 等
    └── RDD 正常计算
    └── 计算完成后，将结果写入 HDFS checkpoint 目录
    └── 写入后，清理 RDD 的血缘信息

  第三阶段：使用（后续 action）
    └── RDD 直接读取 HDFS checkpoint 数据
    └── 不再依赖父 RDD
    └── dependencies.length = 0
```

```scala
// ========== Checkpoint 完整示例 ==========

// 第一步：设置 checkpoint 目录（必须在 checkpoint 之前设置）
// HDFS 路径，保证数据可靠存储
sc.setCheckpointDir("hdfs:///tmp/spark-checkpoint")

// 第二步：构建复杂 RDD 血缘链
val rawRDD = sc.textFile("hdfs://data/clickstream/")
val rdd = rawRDD
  .map(parseClickLog)                    // tranformation 1
  .filter(_.isValid)                     // transformation 2
  .keyBy(_.userId)                       // transformation 3
  .mapValues(extractFeatures)            // transformation 4
  .reduceByKey(mergeFeatures)            // transformation 5
  .filter(_._2.featureCount > 10)        // transformation 6
  .map(formatOutput)                     // transformation 7

// 第三步：标记 checkpoint
rdd.checkpoint()

// 第四步：触发计算 — checkpoint 在 count() 执行完毕后写入
println(s"总记录数：${rdd.count()}")

// 此时 rdd 的血缘已经被斩断
println(s"依赖数量：${rdd.dependencies.length}")  // 输出：0

// 后续操作直接基于 checkpoint 数据
rdd.saveAsTextFile("hdfs://output/result/")
// 不会从头计算了，直接从 HDFS checkpoint 目录读取
```

### Cache vs Checkpoint — 全面对比

很多初学者把 cache 和 checkpoint 搞混，这里用一张对比表帮大家理清楚：

| 对比维度 | Cache | Checkpoint |
|---------|-------|------------|
| **存储位置** | Executor 内存或本地磁盘 | HDFS（可靠存储，多副本） |
| **血缘关系** | 保留完整血缘 | 斩断血缘，dependencies 清空 |
| **生命周期** | 应用结束或调用 unpersist 后释放 | 手动清理（hdfs dfs -rm） |
| **容错方式** | 丢失后根据血缘重新计算 | 直接从 HDFS 读取 checkpoint 文件 |
| **序列化** | 可选（取决于 StorageLevel） | 总是序列化后写入 HDFS |
| **触发时机** | action 时边计算边缓存 | action 时先计算完再额外写入 HDFS |
| **性能开销** | 低（内存读写） | 高（HDFS 写入 + 序列化） |
| **首次使用** | 不需要额外触发 | 需要一次 action 来完成写入 |
| **使用场景** | 中间结果复用，同一个 stage 内多次使用 | 复杂 DAG 截断血缘，跨作业容错 |

**选择建议**：

```
大部分场景 → Cache（性能好，够用了）

复杂 DAG，血缘链超过 10 层 → Cache + Checkpoint 配合使用
  └── 先 cache（避免 checkpoint 的重复计算）
  └── 再 checkpoint（截断血缘）
  └── 最后 unpersist（释放 cache 空间）

跨作业共享中间结果 → Checkpoint（存 HDFS）
```

> **面试点**：面试官问"Cache 和 Checkpoint 能一起用吗？"答案是**可以，而且推荐一起用**。如果不先 cache，checkpoint 写入时需要从源头重新计算一遍数据；如果先 cache，checkpoint 直接从缓存读，速度快很多。所以最佳实践是：`rdd.cache(); rdd.checkpoint(); rdd.count(); rdd.unpersist()`。

### 踩坑经验：Checkpoint 的注意事项

1. **Checkpoint 目录要设置到 HDFS**：如果设置到本地路径，在集群模式下各 Executor 写入各自的本地磁盘，Driver 读不到
2. **首次使用 checkpoint 的 RDD 会算两次**：所以要配合 cache 使用
3. **不要对频繁 change 的数据做 checkpoint**：checkpoint 文件不会自动清理，要及时手动清理避免 HDFS 被打满
4. **Checkpoint 不等同于持久化**：它是为了截断血缘，不是为了复用——虽然它客观上也能起到复用的效果

## 面试高频考点

### Q: 什么时候用 MEMORY_AND_DISK？

当数据量刚好超过内存时。这是一个很常见的场景——你估算一个 RDD cache 大概需要 2GB，Executor 的 Storage 内存正好 2GB，但实际上各种 overhead 导致不够。

MEMORY_ONLY 在内存不足时会频繁逐出数据，每次使用都需要重新计算。一个 10 分钟的迭代任务，因为反复 evict 和 recompute，可能跑成 30 分钟。

MEMORY_AND_DISK 把多出的部分写磁盘，虽然读磁盘比读内存慢，但比"重新计算"快得多——特别是当 RDD 的血缘链比较长的时候。

### Q: cache 和 persist 的区别？

这是 Spark 面试的"热身题"。`cache()` 内部调用就是 `persist(StorageLevel.MEMORY_ONLY)`，没有任何区别。`persist()` 的灵活性在于你可以传其他 StorageLevel。但要注意，无论是 cache 还是 persist，**都是懒执行**——必须等到 action 操作才会真正缓存数据。

### Q: 为什么要设计内存的动态借用机制？

统一内存管理之前，Storage 和 Execution 各自固定比例，各用各的。这导致了几个典型问题：

```
典型问题场景：
  Executor 内存 8GB，Storage = 2GB，Execution = 2GB

  场景 A：缓存占满 2GB Storage，Shuffle 需要 3GB Execution
    → Execution 只有 2GB → 溢写磁盘 → 性能下降
    → Storage 的 2GB 用不上（Shuffle 不需要缓存）

  场景 B：Shuffle 完成，需要缓存 2.5GB 结果到 Storage
    → Storage 只有 2GB → 只能缓存一部分
    → Execution 的 2GB 已经空闲了，但 Storage 借不了
```

动态借用机制让 Execution 可以从 Storage 借用内存，当 Storage 空闲时也可以被 Execution 使用，显著提高整体内存利用率。

### Q: Spark OOM 的原因和排查？

Spark OOM 是生产环境最让人头疼的问题之一。下面是四种最常见的 OOM 类型及排查方法：

| OOM 类型 | 典型原因 | 解决方案 |
|---------|---------|---------|
| **Driver OOM** | `collect()` 拉取数据太多，Driver 装不下 | 增大 `driver-memory`；不要用 `collect()`，改用 `take(N)` 或写入 HDFS |
| **Executor OOM** | 单个 partition 数据太大，超出 Executor 内存 | 增大分区数：`spark.sql.shuffle.partitions` 或 `rdd.repartition()` |
| **Shuffle OOM** | reduce 端需要大量内存合并 shuffle 数据 | 减小 `spark.sql.shuffle.partitions` 让每个 partition 更小；或增大 executor memory |
| **UDF OOM** | UDF 中缓存了过多数据（例如用 HashMap 做 mapping） | 检查 UDF 实现，不要在 UDF 内部创建大规模集合对象 |

**生产环境排查三步法**：

```
Step 1：看日志 — 找到 OOM 发生的位置（哪个 stage、哪个 task）
Step 2：看监控 — 该 Executor 的 GC 频率、堆内存使用趋势
Step 3：看代码 — 对应的 stage 在做什么操作，有没有不合理的 collect 或 cache
```

## 小结

| 概念 | 要点 | 一句话口诀 |
|------|------|-----------|
| 统一内存 | Storage / Execution 动态借用，单向借用机制 | Execution 可以抢，Storage 只能等 |
| 堆外内存 | 避免 GC，适合 Tungsten 二进制格式 | 无 GC 但手动管 |
| 持久化 | 多次使用的 RDD 必须 cache | 三次以上，务必 cache |
| Checkpoint | 斩断血缘，适合复杂 DAG | 先 cache 再 checkpoint |
| 存储级别选择 | 优先 MEMORY_ONLY，不够用 MEMORY_AND_DISK | 内存优先，磁盘兜底 |
| 踩坑总结 | 注意 300MB Reserved，注意动态借用的缓存震荡 | 默认值不是万能的 |
