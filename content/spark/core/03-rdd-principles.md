# Spark Core — RDD 核心原理

## RDD 是什么

### 为什么需要 RDD？

在 Hadoop MapReduce 时代，开发者面临着几个非常痛苦的问题：

- **计算模型太底层**：想做一个简单的 join，你得手动写 Mapper 和 Reducer，代码量巨大且逻辑分散在多个类里
- **中间结果全落盘**：Map 的输出要写到磁盘，Reduce 再读回来，每个 MR 任务之间也依赖 HDFS 中转。对于迭代计算（比如机器学习算法），这几乎是灾难性的性能瓶颈
- **表达能力弱**：MR 的编程模型只提供 map + reduce 两个算子，复杂的 DAG（有向无环图）计算需要拼凑多个 MR 链，开发和维护成本极高

> **面试点**：面试官问"为什么 Spark 比 MapReduce 快"，除了提内存计算，一定要讲 RDD 的惰性求值 + DAG 优化 + 流水线执行，三者缺一不可。

RDD（Resilient Distributed Dataset，弹性分布式数据集）是 Spark 最核心的抽象，表示一个**不可变、分区的、可并行操作**的数据集合。它不是什么"改进了的 MR 框架"，而是一种全新的数据抽象——让开发者像操作本地集合一样操作分布式数据。

```
RDD 五大特性：
┌─────────────────────────────────────────────────┐
│ 1. Partition 列表          → 数据分片             │
│ 2. 每个分区的 compute 函数   → 计算逻辑           │
│ 3. 依赖列表（Dependencies）  → 血缘关系           │
│ 4. Partitioner（可选）      → K-V RDD 的分区器    │
│ 5. 首选位置（可选）          → 数据本地性           │
└─────────────────────────────────────────────────┘
```

这五大特性每一个都不是凭空设计的，它们对应分布式系统中最核心的关切点：

| 特性 | 解决什么问题 | 类比理解 |
|------|------------|---------|
| Partition 列表 | 数据太多，一台机器装不下 | 一本书被拆成多册 |
| compute 函数 | 每个分片怎么算 | 每册书的阅读指南 |
| Dependencies | 算到一半挂了怎么办 | 知道这本书是从哪本书抄来的 |
| Partitioner | 数据怎么分组（避免全量 Shuffle） | 分册的索引目录 |
| Preferred Locations | 数据在哪，计算去哪 | 书在哪个书架上 |

### RDD 是不可变的

```scala
val rdd = sc.parallelize(1 to 100)
// rdd.map(...)  创建新的 RDD，原 RDD 不变
// RDD 上的所有转换（transformation）都返回新 RDD
```

> **为什么不可变**：分布式场景下，修改数据的一致性无法在实时保证（需要分布式事务）。不可变 + 重新计算是更简单正确的模型。

**踩坑经验**：如果你在写 Spark 代码时发现使用了 `var rdd` 并反复赋值，通常说明你的设计有问题。正确的做法是利用 RDD 的不可变性构建一条计算链：

```scala
// ❌ 不好的写法
var rdd = sc.textFile("data.log")
rdd = rdd.filter(_.contains("ERROR"))
rdd = rdd.map(parseLine)

// ✅ 好的写法 — 链式调用，RDD 天然不可变
val result = sc.textFile("data.log")
  .filter(_.contains("ERROR"))
  .map(parseLine)
```

不可变的另一个重要推论是：**同一份 RDD 可以安全地被多个算子共享**，不用担心某个算子修改了数据影响其他算子。

### RDD 是惰性求值的

```scala
val rdd = sc.textFile("hdfs://data.log")
  .filter(_.contains("ERROR"))
  .map(line => (parseLevel(line), 1))
  .reduceByKey(_ + _)
// ← 到这里什么计算都没有发生

rdd.saveAsTextFile("hdfs://output")  // ← Action！触发实际计算
```

> **惰性的原因**：给 Spark 机会全图优化——DAG 调度器可以从全部算子中做列裁剪、谓词下推等优化。

如果 RDD 是"急性的"（立即求值），那么 `filter` 后产生 RDD1，`map` 后产生 RDD2，每个中间结果都会被完整计算和存储——这带来的 IO 和 GC 开销是巨大的。而惰性求值让 Spark 有机会：

1. **合并算子**：连续的窄依赖算子可以合并到一个 Stage 中 pipeline 执行
2. **裁剪计算**：如果最终只需要某几列，在读取阶段就跳过无关列
3. **选择最优执行路径**：同一个算子可能有多种执行策略

我们来看一个对比来加深理解：

```scala
// 假设有 1 亿行数据
val rdd = sc.textFile("hdfs://bigdata.log")

// 如果不惰性求值：先 filter 出一个 1000 万行的中间 RDD，
// 再对这个中间 RDD 做 map —— 中间结果的内存/IO 开销极大！
val filtered = rdd.filter(_.contains("ERROR"))
val mapped = filtered.map(extractFields)
// 惰性求值 = 上面的操作什么都没做

// 直到这里才真正计算
mapped.saveAsTextFile("hdfs://output")
// 实际执行：textFile → filter → map → saveAsTextFile
// 一个 pipeline 完成，没有中间落盘
```

**惰性求值和容错的关系**：因为 RDD 记录了完整的血缘（Lineage），惰性求值 + 血缘的组合保证了即使某个分区丢失，Spark 也知道如何重算——而且是**从最近的可用 checkpoint 或缓存开始重算**，不是从头算。

## RDD 血缘（Lineage）

### 血缘的本质

血缘（Lineage）是 RDD 的**容错基石** — 记录了每个 RDD 如何从父 RDD 和原始数据计算而来。可以把它理解为一个"家谱"或者"食谱"——你知道每道菜（RDD）的原材料是什么以及怎么做出来的，那么中间某个菜被吃了（分区丢失），照着食谱再来一遍就可以了。

```
sc.textFile("hdfs://...")
     ↓ (MapPartitionsRDD)
  .filter(_.contains("ERROR"))
     ↓ (MapPartitionsRDD)
  .map(line => (key, 1))
     ↓ (ShuffledRDD)
  .reduceByKey(_ + _)

如果 reduceByKey 的某个分区丢失 →
Spark 只需要从 .map 开始重新计算该分区
不需要从头读取 HDFS！
```

> **面试点**：RDD 的容错和 MR 的容错有什么区别？
>
> MR 的容错是"粗粒度"的——Task 挂了整个 Task 重跑，从 HDFS 重新读输入。RDD 的容错是"细粒度"的——只重算丢失的那个分区，而且可以从血缘链上最近的 checkpoint 开始重算，不是必须从源头。这就是为什么血缘 + 惰性求值比 MR 的"重试整个 Task"高效得多。

### 血缘在实际项目中的应用

假设你在做一个日志分析系统，每天处理 500 GB 的 Nginx 日志：

```scala
val raw = sc.textFile("hdfs://logs/2025-01-15/")
val parsed = raw.map(parseNginxLine)
val valid = parsed.filter(_.statusCode != 0)
val hourlyStats = valid
  .map(log => (log.hour, 1))
  .reduceByKey(_ + _)

// 如果此时 hourlyStats 的一个分区丢失了（Executor 挂了），
// Spark 会从 valid 开始重算，而不是从 raw 开始
// 因为 Spark 的依赖链知道 hourlyStats 只依赖于 valid
```

### Stage 划分（面试必考！）

Spark 遇到**宽依赖**就切割 Stage，这是 Spark 作业调度的核心机制：

- **窄依赖**：父 RDD 的每个分区最多被一个子 RDD 分区使用（不需要 Shuffle）
  - `map`、`filter`、`flatMap`、`mapPartitions`
  - 父分区 → 子分区 1:1 或 N:1
  - 可以 pipeline 执行（多个算子在一个 Task 内连续执行）

- **宽依赖**：父 RDD 的一个分区被多个子 RDD 分区使用（需要 Shuffle）
  - `reduceByKey`、`groupByKey`、`groupBy`、`join`（非 broadcast）
  - 父分区 → 子分区 1:N
  - 需要等待所有父分区数据都 Shuffle 结束后才能开始子 Stage

| 对比维度 | 窄依赖 | 宽依赖 |
|---------|--------|--------|
| 数据移动 | 无网络传输 | 需要网络 Shuffle |
| 容错恢复 | 只需重算父 RDD 的对应分区 | 需要重算所有父分区 |
| 执行效率 | 极高（pipeline 执行） | 较低（网络开销 + 磁盘 IO） |
| 典型算子 | map, filter, flatMap | reduceByKey, groupByKey, join |
| Stage 边界 | 不产生边界 | 切分 Stage 的边界 |

```
Stage 划分过程（从后往前）：

Job:
  textFile → filter → map → reduceByKey → map → collect

                         ↑ 遇到宽依赖，切一刀
 Stage 0:  textFile → filter → map
 Stage 1:              reduceByKey → map
 Stage 2:                           collect

每个 Stage 内的窄依赖算子可以流水线执行（pipeline）
宽依赖必须等上一个 Stage 全部完成
```

**踩坑经验——怎么看出 Stage 数量多了？**

在 Spark UI 的 DAG 可视化中，你看到的每个方框就是一个 Stage。如果 Stage 数量远多于预期，通常意味着：

1. 存在不必要的宽依赖（比如能用 `reduceByKey` 的地方用了 `groupByKey`，或者频繁 `repartition`）
2. 算子顺序没有优化（比如可以先 `filter` 再 `join`，避免大表 Shuffle）

优化的原则是：**尽可能延后宽依赖，尽可能提前窄依赖中的过滤操作**。

```scala
// ❌ 低效做法 — 先 join 再 filter，大表参与 Shuffle
bigRdd.join(smallRdd).filter(_._2.score > 60)

// ✅ 高效做法 — 先 filter 减少数据量再 join
bigRdd.filter(_._2.score > 60).join(smallRdd)
```

### 宽窄依赖的判断方法

```scala
// 窄依赖：每个分区的父 RDD 分区是确定的
rdd1.map(f)           // 1:1，确定
rdd1.flatMap(f)       // 1:1，确定
rdd1.filter(f)        // 1:1，确定
rdd1.union(rdd2)      // N:1（多个父分区 -> 一个子分区），确定

// 宽依赖：子分区需要多个父分区的数据
rdd1.groupByKey()     // 1:N，不确定
rdd1.reduceByKey(f)   // 1:N，不确定（虽然 map 端预聚合了，但仍然是宽依赖）
rdd1.join(rdd2)       // 1:N，不确定
```

## 算子分类

### Transformation（转换，惰性）

Transformation 是 RDD 的"食谱步骤"——每调用一个 Transformation，就相当于在食谱上多写一步。菜还没做（计算没触发），但步骤已经记全了。

| 算子 | 类型 | 说明 | 适用场景 |
|------|------|------|---------|
| `map(f)` | 窄 | 一对一映射 | 数据类型转换、字段提取 |
| `flatMap(f)` | 窄 | 一对多展开 | 分词、拆分 JSON 数组 |
| `filter(f)` | 窄 | 过滤 | 日志筛选、无效数据剔除 |
| `mapPartitions(f)` | 窄 | 按分区批量操作 | 批量创建数据库连接、批量写入 |
| `distinct()` | 宽 | 去重（需要 Shuffle） | 数据去重 |
| `reduceByKey(f)` | 宽 | 按键聚合（有 Map 端预聚合） | WordCount、分组统计 |
| `groupByKey()` | 宽 | 按键分组（无预聚合） | 全量分组（**尽量不用**） |
| `sortByKey()` | 宽 | 按键排序 | TopN 分析、报表排序 |
| `join(other)` | 宽 | 内连接（可优化为窄） | 多表关联、维度扩充 |
| `repartition(n)` | 宽 | 重分区（全量 Shuffle） | 增加分区数 |
| `coalesce(n)` | 窄 | 减少分区（默认不 Shuffle） | 减少小文件、合并结果 |

> **面试点**：`reduceByKey` vs `groupByKey` 的区别是高频考点！
>
> `reduceByKey` 在 Map 端做了**预聚合（map-side combine）**，相同 key 先在 Map 端合并，大大减少 Shuffle 数据量。`groupByKey` 不做预聚合，所有数据原样 Shuffle。**能用 `reduceByKey` 的情况下，永远不要用 `groupByKey`**。

```scala
// reduceByKey — 预聚合版本（推荐）
val wordCount = text
  .flatMap(_.split(" "))
  .map(word => (word, 1))
  .reduceByKey(_ + _)
  // Map 端合并：(hello, 1) + (hello, 1) => (hello, 2)
  // Shuffle 时传输的是合并后的结果

// groupByKey — 无预聚合版本（不推荐）
val wordCount = text
  .flatMap(_.split(" "))
  .map(word => (word, 1))
  .groupByKey()
  .mapValues(_.sum)
  // Shuffle 时传输所有 (hello, 1)，不做预聚合
```

### Action（动作，触发计算）

Action 是"烹饪"的指令——调用 Action 的那一刻，Spark 才开始真正干活。之前堆叠的一堆 Transformation 会在这个点被编译成一个 DAG 执行计划并提交。

| 算子 | 说明 | 常见踩坑 |
|------|------|---------|
| `collect()` | 拉取到 Driver | **大表 collect 会 OOM！** 只用于结果确认或调试 |
| `count()` | 行数 | 大规模数据 count 可能很慢，考虑采样估算 |
| `take(n)` | 取前 n 条 | 不触发完整计算（局部采样），很快 |
| `first()` | 第一条 | 等价于 `take(1)` |
| `reduce(f)` | 归约 | 注意空 RDD 会抛异常 |
| `foreach(f)` | 遍历 | 算子内操作在 Executor 执行，不是 Driver |
| `saveAsTextFile(path)` | 保存 | 输出大量小文件时用 `coalesce` 合并 |
| `countByKey()` | 按键统计 | 返回 Map，key 量大也会 OOM |

**踩坑经验——`collect()` 如何正确使用？**

```scala
// ❌ 绝对不要这么干！
val allData = rdd.collect()  // 1 亿行全拉到 Driver，必 OOM

// ✅ 正确做法：只取少量预览
val sample = rdd.take(10)
sample.foreach(println)

// ✅ 如果确实需要全量数据到 Driver（数据量可控）
val result = rdd.filter(isImportant).collect()
```

## 共享变量

### 为什么需要共享变量？

先看一个常见的错误代码：

```scala
// ❌ 这段代码不会按你想象的方式工作！
val lookupTable = Map(1 -> "张三", 2 -> "李四")
val result = rdd.map(record => (record.id, lookupTable(record.id)))
```

问题出在哪？`lookupTable` 在 Driver 端创建，但 `map` 算子里的代码在 **Executor 的每个 Task** 中执行。上面的写法意味着每个 Task 都会通过网络从 Driver 拉取一份 `lookupTable` 的副本。如果有 1000 个 Task，`lookupTable` 被序列化并传输了 1000 次！

共享变量就是 Spark 为解决这类问题提供的神器。

### Broadcast（广播变量）

```scala
val smallTable = Map(1 -> "张三", 2 -> "李四", 3 -> "王五")
val broadcastTable = sc.broadcast(smallTable)

largeRDD.map { record =>
  val lookup = broadcastTable.value  // 从本地内存读取
  (record, lookup.getOrElse(record.userId, "未知"))
}
```

**原理**：数据只发送到每个 Executor 一次（不是每个 Task 一次），存在 Executor 内存中。Executor 内的所有 Task 共享这份数据，无序列化开销。

| 对比项 | 普通闭包变量 | Broadcast 变量 |
|--------|------------|---------------|
| 传输次数 | 每个 Task 一次 | 每个 Executor 一次 |
| 存储位置 | Task 的序列化数据中 | Executor 的共享内存中 |
| 100 个 Executor × 10 Task | 1000 次传输 | 100 次传输 |
| 1000 个 Executor × 10 Task | 10000 次传输 | 1000 次传输 |

**使用条件**：
- 数据能放入 Executor 内存（通常 < 几百 MB）
- 在多个 Task 中只读使用
- 比 Shuffle Join 的效率高一个量级

**实战示例：用 Broadcast 做 Map-Side Join**

```scala
// 场景：大表（用户行为日志）join 小表（城市映射表，500 条）
// 数据：行为日志 10 亿行，城市映射 500 条

// ❌ 常规 Join — 触发 Shuffle，10 亿行都要网络传输
val shuffledJoin = behaviorLog.join(cityMap)

// ✅ Broadcast Join — 无 Shuffle，小表广播到每个 Executor
val broadcastCities = sc.broadcast(cityMap.collectAsMap())
val mapSideJoin = behaviorLog.map { log =>
  val cityName = broadcastCities.value.getOrElse(log.cityId, "未知")
  (log.userId, log.eventType, cityName)
}
// 网络传输量：cityMap 的 500 条数据 × Executor 数
// 远小于 10 亿条数据的 Shuffle！
```

> **面试点**：Broadcast Join 的条件是什么？
>
> 1. 一个小表（通常小于 1 GB，Spark 自动阈值由 `spark.sql.autoBroadcastJoinThreshold` 控制，默认 10 MB）
> 2. 大表和小表做 Join
> 3. 小表只读不改
> 4. 可以手动调大自动广播阈值或强制 hint：`/*+ BROADCAST(t) */`

### Accumulator（累加器）

累计器解决的是一个很朴素的问题：**你有一个大 RDD 需要全量处理，过程中想统计一些指标（比如有多少条 ERROR 日志），但 foreach 中不能直接加 Driver 端的变量**。

```scala
val errorCount = sc.longAccumulator("error_counter")
val totalCount = sc.longAccumulator("total_counter")

rdd.foreach { record =>
  totalCount.add(1)
  if (record.level == "ERROR") errorCount.add(1)
}

println(s"ERROR 占比: ${errorCount.value * 100.0 / totalCount.value}%")
```

**注意**：
- Task 端只能 `add`，不能读取值
- Driver 端可以 `.value` 读取
- Transformation 中的 accumulator 可能因重试被多次计数

**踩坑经验——Accumulator 在 Transformation 中的陷阱**：

```scala
// ⚠️ 注意：以下代码中 accumulator 可能不准确！
val acc = sc.longAccumulator("count")
val transformed = rdd.map { x =>
  acc.add(1)  // 在 Transformation 中操作 accumulator
  x * 2
}

// 如果此处 Executor 挂了，Task 重试，
// acc 会被重复累加！
transformed.saveAsTextFile("output")
println(acc.value)  // 可能比实际数据量大
```

**最佳实践**：

| 用法 | 准确性 | 说明 |
|------|--------|------|
| Action 中用 Accumulator | **精准** | Task 执行一次，不会重试 |
| Transformation 中用 Accumulator | **不准确** | 重试会导致重复计数 |
| Driver 读取 `.value` | **只读** | 仅用于 Driver 端汇总 |

> **面试点**：为什么 Transformation 中的 Accumulator 可能不准确？
>
> 因为 Spark 的容错机制——如果一个 Task 挂了，Spark 会在另一个 Executor 上重试。Transformation 中的 Accumulator 在重试时再次 `add`，导致重复计数。而 Action 中的 Task 是不重试的（或者重试时不会重复执行），所以 Action 中的 Accumulator 是准确的。

### 自定义 Accumulator

除了 `longAccumulator` 和 `doubleAccumulator`，你还可以自定义：

```scala
import org.apache.spark.util.AccumulatorV2

class SetAccumulator extends AccumulatorV2[String, Set[String]] {
  private var set = Set.empty[String]

  def add(v: String): Unit = set += v
  def value: Set[String] = set
  def isZero: Boolean = set.isEmpty
  def copy(): SetAccumulator = { val a = new SetAccumulator; a.set = set; a }
  def reset(): Unit = set = Set.empty[String]
  def merge(other: AccumulatorV2[String, Set[String]]): Unit =
    set ++= other.asInstanceOf[SetAccumulator].set
}

// 使用
val uniqueIPs = new SetAccumulator
sc.register(uniqueIPs, "unique_ips")
rdd.foreach(record => uniqueIPs.add(record.ip))
println(s"独立 IP 数: ${uniqueIPs.value.size}")
```

## 持久化与缓存

### 为什么需要手动缓存？

RDD 的血缘虽好，但也带来了一个问题：如果你对同一个 RDD 做多次 Action，每次 Action 都会从源头开始重新计算整个血缘链！

```scala
val logs = sc.textFile("hdfs://bigdata.logs")
val parsed = logs.map(parseLog)

// 第一次 Action
println(s"日志总数: ${parsed.count()}")    // 从头开始算 map

// 第二次 Action
val errorCount = parsed.filter(_.level == "ERROR").count()
// 又从头开始算 map！而且上面 count 的结果完全浪费了
```

只要没有显式地 `cache()` 或 `persist()`，每次 Action 都会重新走一遍从源头到终点的计算链条。这在迭代计算（比如机器学习）和多次查询的场景下是灾难性的。

### 缓存级别详解

```scala
// 缓存级别
rdd.cache()           // MEMORY_ONLY（默认）
rdd.persist(StorageLevel.MEMORY_AND_DISK)
rdd.persist(StorageLevel.DISK_ONLY)
rdd.persist(StorageLevel.MEMORY_ONLY_SER)  // 序列化存储（省内存）

// 移除缓存
rdd.unpersist()

// 检查点（checkpoint）：截断血缘
sc.setCheckpointDir("hdfs://checkpoint")
rdd.checkpoint()
```

| 缓存级别 | 位置 | 序列化 | 说明 | CPU 开销 | 适用场景 |
|---------|------|--------|------|---------|---------|
| MEMORY_ONLY | 内存 | 否 | 默认，最快但最占内存 | 低 | 数据量小、内存充足 |
| MEMORY_AND_DISK | 内存+磁盘 | 否 | 内存不够溢写磁盘 | 低 | 数据量大、可容忍磁盘 IO |
| MEMORY_ONLY_SER | 内存 | 是 | 省内存、多 CPU 开销 | 中 | 内存紧张、CPU 有余 |
| MEMORY_AND_DISK_SER | 内存+磁盘 | 是 | 内存+磁盘，全部序列化 | 中 | 大数据的常规选择 |
| DISK_ONLY | 磁盘 | 是 | 最慢但最安全 | 高 | 完全不信任内存时 |

**实战选型建议**：

```scala
// 1. 数据量少于内存的 70% → MEMORY_ONLY
val moderateRDD = data.filter(isValid).cache()

// 2. 数据量大于内存 → MEMORY_AND_DISK
val largeRDD = data.flatMap(extractFeatures).persist(
  StorageLevel.MEMORY_AND_DISK
)

// 3. 数据量大且内存极紧张 → MEMORY_ONLY_SER（需要 Kyro 序列化）
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
val bigRDD = data.map(transform).persist(
  StorageLevel.MEMORY_ONLY_SER
)
```

### Checkpoint 与 Cache 的区别

很多新手被这两个概念搞混，这里用一张表说清楚：

| 对比项 | cache / persist | checkpoint |
|--------|----------------|------------|
| 存储位置 | Executor 内存或磁盘 | HDFS（或其他可靠存储） |
| 生命周期 | 随 App 结束 | 持久化到文件系统，跨 App 可用 |
| 血缘截断 | **不截断**，仍然保留完整血缘 | **截断血缘**，相当于新的数据源头 |
| 是否序列化 | 取决于级别 | 始终序列化 |
| 触发时机 | Action 时自动缓存 | Action 时先计算再单独保存 |
| 使用场景 | 多次使用同一 RDD | 切断过长血缘、跨 App 共享数据 |

**什么时候该用 checkpoint？**

```scala
// 场景：超长血缘链，几十层 Transformation
val step10 = step09.join(step08)
  .filter(_._2.nonEmpty)
  .mapValues(process)
  .reduceByKey(...)
  // ... 再经过十几步
// 此时血缘链已经非常长，任何一个分区丢失，
// 重算需要从 step10 一直追溯到 step01 甚至数据源

// 在关键节点 checkpoint 截断血缘
sc.setCheckpointDir("hdfs://checkpoint-dir/")
step10.checkpoint()
// 之后某个分区丢失，Spark 直接读取 checkpoint 数据
// 不用重算 step01 ~ step10 的完整链条
```

### 缓存的最佳实践

```scala
// ✅ 好做法 — 迭代计算中缓存中间 RDD
var nodes = sc.parallelize(initialNodes)
for (i <- 1 to 100) {
  nodes = nodes.flatMap(expandNeighbors)
    .reduceByKey(mergeScores)
  nodes.cache()    // 每次迭代的新 RDD 都缓存
  nodes.count()    // Action 触发缓存
}

// ✅ 好做法 — 边缘 RDD 不要缓存（只用到一次）
val tmp = rdd.filter(_.isTemp)   // 只用一次，不要 cache
val important = rdd.filter(_.isVIP).cache()  // 多次使用，cache

// ❌ 坏做法 — 缓存用不到的 RDD
val neverUsed = rdd.cache()  // 缓存了但不使用
val result = rdd.map(f).collect()
```

## 面试高频考点

### Q: RDD 五大特性？

1. 一系列 Partition（分片）
2. compute 函数（对每个分区的计算）
3. Dependencies 列表（血缘）
4. Partitioner（K-V 才有）
5. PreferredLocations（数据本地性）

### Q: 宽依赖和窄依赖的区别？为什么要区分？

窄依赖可以流水线并行（同一 Stage 内），宽依赖必须 Shuffle 并切割 Stage。区分是为了 DAG 调度和容错——窄依赖的失败只需重新计算对应分区，宽依赖可能需重新计算多个 Stage。

### Q: `reduceByKey` 和 `groupByKey` 的区别？为何前者更快？

`reduceByKey` 在每个 Map 端先做本地聚合（Combiner），Shuffle 数据量小。`groupByKey` 将所有原始数据 Shuffle。前者 Shuffle 数据量通常是后者的 1/10 甚至更低。

### Q: `repartition` 和 `coalesce` 的区别？

- `repartition(n)` = `coalesce(n, shuffle = true)` — 可以增或减分区，全量 Shuffle
- `coalesce(n)` = `coalesce(n, shuffle = false)` — 只能减分区，不 Shuffle（合并相邻分区）

### Q: RDD vs DataFrame vs Dataset？

| 维度 | RDD | DataFrame | Dataset |
|------|-----|-----------|---------|
| 类型安全 | 编译期 | 运行时 | 编译期 |
| 优化 | 无 | Catalyst | Catalyst + Tungsten |
| 序列化 | Java | Tungsten 堆外 | Encoder |
| API 风格 | 函数式 | SQL | 函数式 + SQL |
| 适用场景 | 非结构化数据 | SQL/ML 流水线 | 强类型场景 |

## 小结

| 核心概念 | 关键点 |
|---------|--------|
| RDD 不可变 | 分布式一致性的前提，修改 = 新建 |
| 惰性求值 | 给 Spark 全图优化机会 |
| 血缘 | 容错机制，分区丢失只需重新计算依赖链 |
| 宽/窄依赖 | 决定 Stage 边界和 Shuffle 点 |
| 共享变量 | Broadcast（广播大变量）+ Accumulator（分布式计数） |
| 缓存策略 | 被多次使用的 RDD 必须缓存 |

### 面试核心考点速查

> - **"Spark 为什么快"**：RDD 内存计算 + 惰性求值 DAG 优化 + pipeline 执行 + 容错细粒度
> - **"Stage 怎么划分"**：遇到宽依赖（Shuffle）就切一刀，从后往前推
> - **"reduceByKey vs groupByKey"**：前者有 map-side combine，后者没有；能不用 groupByKey 就别用
> - **"Broadcast 原理"**：每个 Executor 一次，不是每个 Task 一次；适合小表 join 大表
> - **"Accumulator 准确吗"**：Action 中准确，Transformation 中可能因重试重复计数
> - **"cache vs checkpoint"**：cache 不截断血缘，checkpoint 截断；cache 在内存，checkpoint 在 HDFS
