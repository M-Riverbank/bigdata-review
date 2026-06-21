# Spark Core — 共享变量与累加器

## 广播变量（Broadcast Variable）

### 为什么需要广播变量？

在分布式计算中，Driver 定义的普通变量会在每个 Task 中序列化一份副本。这是个非常容易被忽视的性能陷阱。

```scala
// ❌ 坏做法：每个 Task 都发送一份 referenceTable
val referenceTable = Map("A" -> 1, "B" -> 2, "C" -> 3)
rdd.map { key =>
  (key, referenceTable.getOrElse(key, 0))
}
// 如果有 1000 个 Task → referenceTable 被序列化 1000 次！
```

> **面试点**：这是 Spark 面试中「你知道哪些性能优化手段？」的标准答案之一。关键在于理解 Task 与 Executor 的关系——广播变量减少的是网络传输量，而不是计算量。

**底层原理**：当 Driver 将 Job 分发到 Executor 时，每个 Task 都包含自己的闭包（closure）。闭包中引用了外部变量，这些变量会随着 Task 序列化一起发送。如果该变量很大（比如一个 100MB 的维表），网络开销会急剧增加，甚至导致 `Task not serializable` 异常。

| 对比维度 | 普通变量 | 广播变量 |
|---------|---------|---------|
| 传输次数 | 每个 Task 一次 | 每个 Executor 一次 |
| 100个Executor × 10个Task | 1000 次序列化 | 100 次（实际更少，分布式缓存） |
| 网络开销 | O(Task数 × 变量大小) | O(Executor数 × 变量大小) |
| 内存占用 | 每个 Task 一份副本 | Executor 内 Task 共享 |

**踩坑经验**：在生产环境中，如果发现 Web UI 的 `Shuffle Write` 不大但 `Input` 特别大，或者 Stage 的 `Scheduler Delay` 异常高，通常就是遗漏了广播变量优化。

### Broadcast 原理

```
Driver 端 largeVariable
  │
  ├──► Executor 1 (BlockManager) — 每个 Executor 存一份
  │     ├── Task A ──► 本地读取（反序列化）
  │     └── Task B ──► 本地读取（反序列化）
  │
  └──► Executor 2 (BlockManager)
        ├── Task C ──► 本地读取
        └── Task D ──► 本地读取
```

**工作流程详解**：

1. **Driver 创建广播变量**：调用 `sc.broadcast(data)` 时，Spark 不会立即发送数据，而是创建一个 `Broadcast` 元数据对象
2. **懒式分发**：只有当 Action 触发 Job 执行时，广播变量才真正随 Task 一起发送
3. **TorrentBroadcast 协议**：Spark 使用类似 BitTorrent 的 P2P 协议分发广播变量，而不是传统的 Driver 逐个推送——第一个拉取的 Executor 拿到完整数据后，会成为新的「种子节点」，后续 Executor 可以从已获取的 Executor 并行下载，避免 Driver 单点瓶颈
4. **BlockManager 缓存**：每个 Executor 的 BlockManager 负责存储广播变量，同一 Executor 内的所有 Task 共享该副本

```scala
// 完整的广播变量生命周期示例
import org.apache.spark.broadcast.Broadcast

// Step 1: 准备数据（Driver 端）
val lookupTable: Map[String, Double] = loadLargeLookupTable()  // 假设 500MB

// Step 2: 创建广播变量
val bcTable: Broadcast[Map[String, Double]] = spark.sparkContext.broadcast(lookupTable)

// Step 3: 在 Transformation 中使用（Executor 端读取）
val enriched = rdd.map { key =>
  val value = bcTable.value.getOrElse(key, 0.0)  // 本地读取，无网络开销
  (key, value)
}

// Step 4: 不再使用时销毁（释放 Executor 内存）
bcTable.destroy()

// Step 5: 可选——取消持久化（如果只想释放特定存储级别）
bcTable.unpersist()
```

> **面试点**：Spark 2.x+ 默认使用 `TorrentBroadcast`，它有两个优势——(1) 避免 Driver 网卡成为瓶颈，(2) 单个 Executor 拉取失败可以从其他 Executor 重试。老版本使用的是 `HttpBroadcast`（已废弃）。

### 广播变量的存储级别

```
Broadcast 的存储策略：
1. 优先存内存（MEMORY_ONLY）
2. 内存不够落磁盘（MEMORY_AND_DISK）
3. 序列化存储（MEMORY_ONLY_SER）— 减少内存占用
```

**各存储级别对比**：

| 存储级别 | 优点 | 缺点 | 适用场景 |
|---------|------|------|---------|
| MEMORY\_ONLY | 读取最快，无需反序列化 | 内存占用大 | 频繁访问的小广播 |
| MEMORY\_AND\_DISK | 内存不足时不会 OOM | 磁盘读取慢 | 不稳定环境、大广播 |
| MEMORY\_ONLY\_SER | 内存效率高（Java 序列化） | 每次读取需反序列化 | CPU 充裕、内存紧张 |

**实践建议**：
- 默认情况下，Spark 会使用 `MEMORY_ONLY` 存储 + 自动压缩（`spark.broadcast.compress=true`，使用 `lz4` 压缩）
- 如果广播变量超过 500MB，建议显式使用序列化存储，并在代码中通过 `bcTable.value` 读取
- 不要频繁创建和销毁广播变量——广播变量在 Executor 端是懒加载的，第一次 `value` 调用才会反序列化

### 广播变量的使用限制

```scala
// ⚠️ 广播变量是只读的！不能在 Executor 端修改
val bc = sc.broadcast(mutable.Map("a" -> 1))

rdd.foreach { key =>
  // ❌ 编译可能通过但运行会出错——广播变量不可变
  // bc.value.update(key, bc.value(key) + 1)
  
  // ✅ 正确做法：读取并创建新的本地结构
  val localCopy = bc.value
  println(localCopy.getOrElse(key, 0))
}
```

**踩坑经验**：
- 广播变量修改后（如重新创建同名变量），需要调用 `unpersist()` 释放旧版本，否则旧 Executor 缓存不会自动清理
- 广播变量不支持更新——如果需要更新版本，创建新的 `Broadcast` 实例并销毁旧的
- 不要广播 Driver 端生成的 RDD 或 DataFrame——这些已经是分布式结构了

### 广播 Join（Map Join）

```scala
// 小表 < 10MB（默认阈值）→ 自动广播
val smallDF = spark.read.parquet("dim_user")
val largeDF = spark.read.parquet("fact_orders")

// 方式一：自动广播（spark.sql.autoBroadcastJoinThreshold = 10MB）
val result1 = largeDF.join(smallDF, "user_id")

// 方式二：显式 broadcast hint
import org.apache.spark.sql.functions.broadcast
val result2 = largeDF.join(broadcast(smallDF), "user_id")
```

> **面试点**：广播 Join 是解决数据倾斜最有效的手段之一。当一个大表 Join 一个小表时，将小表广播到每个 Executor，大表无需 Shuffle。

**广播 Join 的执行过程**：

```
普通 SortMergeJoin (有 Shuffle)：
  largeDF (1TB) ——[Shuffle]——> CoPartitioned RDD
  smallDF (10GB) ——[Shuffle]——> CoPartitioned RDD
  → 两个 Shuffle，大量网络和磁盘 IO

广播 Join (Map Join，无 Shuffle)：
  largeDF (1TB) ——[各分区本地读取]
  smallDF (10GB) ——[Broadcast 到所有 Executor]
  → 一个 Shuffle 都没有！每个 Task 读取 smallDF 广播副本进行本地 Join
```

**参数调优**：

```scala
// 增大广播阈值（如果集群内存充足）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50MB")  // 默认 10MB

// 强制关闭自动广播（仅在调试时使用）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")

// 查看执行计划确认是否触发了广播 Join
val plan = largeDF.join(broadcast(smallDF), "user_id").explain("formatted")
// 输出中应包含 "BroadcastHashJoin" 而非 "SortMergeJoin"
```

**踩坑经验**：
- 自动广播阈值不要设太高——如果小表超过 200MB 还强制广播，Executor 内存会迅速打满，出现 OOM
- 使用 `explain("formatted")` 验证广播 Join 是否生效，Spark 优化器有时会基于统计信息忽略 hint
- 广播 Join 只适用于等值 Join（equi-join），不支持非等值条件

> **面试点**：当大表 Join 大表时，广播 Join 无效。此时可以考虑：分桶（Bucket Join）、Salting（加盐）、或者将大表维度表拆分成多个子表分别广播。

## 累加器（Accumulator）

### 为什么需要累加器？

在分布式计算中，Task 中的普通变量修改无法返回 Driver。

```scala
// ❌ 错误：Task 中的 counter 修改不会传递回 Driver
var counter = 0
rdd.foreach { record =>
  if (record.condition) counter += 1  // 每个 Task 的 counter 是副本！
}
println(counter)  // 仍然是 0
```

**根本原因**：Spark 的算子（如 `map`、`filter`、`foreach`）中的代码在 Executor 端执行，Driver 定义的局部变量会被序列化复制到每个 Task 中。所有修改只发生在 Task 自己的副本上，不会回传给 Driver。

```scala
// 一个更直观的错误示例
var totalLength = 0L
val lengths = sc.parallelize(1 to 10000, 10)  // 10 个分区

lengths.map { x =>
  totalLength += x  // ❌ 每个 Task 独立加自己的副本
  x * 2
}

println(totalLength)  // ❌ 输出 0！Driver 端的 totalLength 从未被修改
```

**正确做法——使用累加器**：

```scala
val totalLengthAcc = sc.longAccumulator("total_length")
val lengths = sc.parallelize(1 to 10000, 10)

lengths.map { x =>
  totalLengthAcc.add(x)  // ✅ 使用累加器
  x * 2
}

println(totalLengthAcc.value)  // ✅ 输出 50005000
```

### 内置累加器

Spark 内置了多种常用累加器，开箱即用：

```scala
// longAccumulator：长整型累加器
val errorCount = sc.longAccumulator("error_counter")
val totalCount = sc.longAccumulator("total_counter")

rdd.foreach { record =>
  totalCount.add(1)
  if (record.level == "ERROR") errorCount.add(1)
}

println(s"Total: ${totalCount.value}, Errors: ${errorCount.value}")
println(s"Error Rate: ${errorCount.value * 100.0 / totalCount.value}%")
```

**内置累加器类型**：

| 累加器 | 创建方式 | 默认值 | 适用场景 |
|--------|---------|--------|---------|
| Long 累加器 | `sc.longAccumulator(name)` | 0L | 计数、求和 |
| Double 累加器 | `sc.doubleAccumulator(name)` | 0.0 | 浮点累加 |
| 集合累加器 | `sc.collectionAccumulator[T](name)` | 空 List | 收集样本数据 |

```scala
// 集合累加器示例：收集异常样本用于调试
import scala.collection.JavaConverters._

val errorSamples = sc.collectionAccumulator[String]("error_samples")
val errorCount = sc.longAccumulator("error_count")

rdd.foreach { record =>
  if (record.status >= 400) {
    errorCount.add(1)
    if (errorCount.value <= 10) {  // 只收集前 10 条样本
      errorSamples.add(s"${record.id}: ${record.status}")
    }
  }
}

println(s"Error count: ${errorCount.value}")
println(s"Samples: ${errorSamples.value.asScala.mkString(", ")}")
```

> **面试点**：累加器的 `value` 只能在 Driver 端读取，Task 端只能 `add`。如果试图在 Task 中调用 `acc.value`，你会得到一个 `SparkException`，提示累加器值不能在 Task 中读取。

### 自定义累加器

当内置累加器不能满足需求时，可以实现 `AccumulatorV2` 来创建自定义累加器。

```scala
import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable

class MaxAccumulator extends AccumulatorV2[Long, Long] {
  private var maxVal = Long.MinValue

  def reset(): Unit = maxVal = Long.MinValue
  def add(v: Long): Unit = { if (v > maxVal) maxVal = v }
  def merge(other: AccumulatorV2[Long, Long]): Unit = {
    add(other.value)
  }
  def value: Long = maxVal
  def copy(): AccumulatorV2[Long, Long] = {
    val n = new MaxAccumulator()
    n.maxVal = maxVal
    n
  }
  def isZero: Boolean = maxVal == Long.MinValue
}

// 注册使用
val maxAcc = new MaxAccumulator()
sc.register(maxAcc, "max_value")
rdd.foreach(v => maxAcc.add(v))
println(s"Max: ${maxAcc.value}")
```

**自定义累加器必须实现的方法**：

| 方法 | 作用 | 调用时机 |
|------|------|---------|
| `reset()` | 重置累加器到初始状态 | Driver 端初始化时 |
| `add(v)` | 添加一个值 | 每个 Task 中的每条记录 |
| `merge(other)` | 合并两个累加器 | Spark 合并各 Task 结果时 |
| `value` | 返回最终值 | Driver 端读取时 |
| `copy()` | 深拷贝累加器 | Spark 序列化/分发时 |
| `isZero` | 判断是否为初始状态 | Spark 判断是否需要合并 |

**更实用的自定义累加器——统计均值**：

```scala
class MeanAccumulator extends AccumulatorV2[Double, (Double, Long, Double)] {
  private var sum = 0.0
  private var count = 0L

  def reset(): Unit = { sum = 0.0; count = 0L }
  def add(v: Double): Unit = { sum += v; count += 1 }
  def merge(other: AccumulatorV2[Double, (Double, Long, Double)]): Unit = {
    sum += other.value._1
    count += other.value._2
  }
  def value: (Double, Long, Double) = (sum, count, if (count == 0) 0.0 else sum / count)
  def copy(): AccumulatorV2[Double, (Double, Long, Double)] = {
    val n = new MeanAccumulator()
    n.sum = sum
    n.count = count
    n
  }
  def isZero: Boolean = count == 0L
}

// 使用
val meanAcc = new MeanAccumulator()
sc.register(meanAcc, "mean_value")
rdd.foreach(v => meanAcc.add(v.toDouble))
val (sum, cnt, avg) = meanAcc.value
println(s"Sum=$sum, Count=$cnt, Mean=$avg")
```

**踩坑经验**：
- 自定义累加器的 `copy()` 必须是**深拷贝**，如果内部有可变集合（如 `mutable.Set`），务必创建新对象
- `isZero` 方法如果实现不正确，Spark 可能会跳过某些分区的合并，导致结果不准
- 累加器必须注册到 `SparkContext` 才能被 Task 识别，否则序列化时抛出异常

### 累加器的限制

```
1. Task 端只能 add，不能读取 value
   → value 读取会返回引用到 Driver 端的内存

2. Transformation 中慎用累加器
   → Task 失败重试会导致累加器重复计数
   → Action 中的累加器是准确的（失败重试不重复）

3. 累加器不是幂等的
   → 如果 Stage 重算，累加器可能被多次更新
```

**关于限制 2 的深入分析**：

```scala
// ❌ Transformation 中使用累加器——结果不可靠
val acc = sc.longAccumulator("bad_example")
val transformed = rdd.map { x =>
  acc.add(1)  // 如果在 map 中使用，acc 会被重复累加
  x * 2
}
// 此时 acc.value 可能是真实值的 2~3 倍（取决于 Stage 重算次数）
val result = transformed.count()
println(s"bad count: ${acc.value}")  // 可能比 rdd.count() 大！

// ✅ Action 中使用累加器——结果精确
val goodAcc = sc.longAccumulator("good_example")
rdd.foreach { x =>   
  goodAcc.add(1)  // Action 中的累加器：Stage 重算不重复
}
println(s"good count: ${goodAcc.value}")  // 等于 rdd.count()
```

**为什么 Transformation 中的累加器会重复？**

Spark 的容错机制基于 RDD Lineage。如果一个 Executor 执行 Task 失败，Spark 会在另一个 Executor 上重算这个 Task。对于 `map`、`filter` 等 Transformation，重算时会重新执行累加器的 `add` 操作，导致重复计数。而 `foreach`、`count` 等 Action 的语义是「最终输出」，Spark 框架确保 Action 级别的计算不会因为重试而重复。

**踩坑经验**：
- Spark UI 的 Stages 页面可以看到 `Skipped Stages` 和 `Retry` 计数——如果看到 Task 有重试，累加器值可能已经不准了
- 使用累加器做监控计数时，建议同时用 `rdd.count()` 或 `df.count()` 做交叉验证
- Spark 3.0+ 引入了 `Accumulator v2` 的 `metadata` 概念，但仍未解决 Transformation 中的幂等问题

## 广播变量 vs 累加器

| 维度 | 广播变量 | 累加器 |
|------|---------|--------|
| 方向 | Driver → Executor | Task → Driver |
| 用途 | 分发大变量给所有 Task | 分布式计数/聚合 |
| 修改 | 只读（Task 不能修改） | Task 只能 add |
| 存储 | BlockManager 内存/磁盘 | Driver 端维护 |
| 典型场景 | 维表广播、ML 模型参数 | 错误计数、总和统计 |
| 容错机制 | TorrentBroadcast P2P 分发 | Task 重试会导致重复计数 |
| 生命周期 | 手动 destroy/unpersist | Job 结束后自动释放 |

## 面试高频考点

### Q: 广播变量太大怎么办？

1. 增大 `spark.broadcast.compress`（默认 true，可使用压缩）
2. 增大 `spark.executor.memory`
3. 分拆为多个小 broadcast
4. 如果数据 > 1GB，考虑用 Redis 等外部存储代替 broadcast

**扩展分析**：

当广播变量超过 2GB 时，即使集群内存充足，也会遇到一个硬限制——Spark 的 TorrentBroadcast 将变量切分为 `spark.broadcast.blockSize`（默认 4MB）的 block，Driver 端需要将所有 block 加载到内存。单个 block 的序列化框架（Java/Kryo）有 2GB 数组上限。

```scala
// 应对超大规模广播的替代方案
// 方案一：基于 Redis 的分布式缓存
import redis.clients.jedis.Jedis

rdd.mapPartitions { iter =>
  val jedis = new Jedis("redis-host", 6379)
  iter.map { key =>
    val value = jedis.get(s"lookup:$key")
    (key, value)
  }
}

// 方案二：分区级加载（避免全部广播）
val bcPaths = sc.broadcast(lookupFilePaths)
rdd.mapPartitions { iter =>
  val localCache = loadFromDisk(bcPaths.value)  // 每个分区从本地磁盘加载
  iter.map { key => (key, localCache.get(key)) }
}
```

### Q: 累加器在 Transformation 中不准怎么办？

答：累加器在 Transformation 中因为 Task 重试可能被多次更新，不是精确一次语义。如果要求精确计数：

1. 将累加器放在 Action（foreach）中使用
2. 用 `mapPartitions` + `Iterator` 模式手动管理
3. 最重要的是：**不在 Transformation 中依赖累加器的精确值**

**最佳实践——mapPartitions + 累加器**：

```scala
// 如果必须在 Transformation 中记录信息，使用 mapPartitions
val acc = sc.longAccumulator("partition_errors")
val result = rdd.mapPartitions { iter =>
  iter.map { x =>
    // 在 mapPartitions 中记录每个元素的信息
    // 但通过本地变量解决重试问题
    x
  }
}
```

> **面试点**：Spark 社区推荐的实践是——不要在 Transformation 中使用累加器做业务逻辑依赖。累加器只适合做**调试和监控**用途，如统计不符合预期的记录数、数据质量监控等。

### Q: `broadcast(emptyDF)` 有什么用？

有时需要通过广播空 DataFrame 来强制触发某些优化，例如在 RDD API 中广播空集合来让 Executor 初始化某些资源连接。

**真实案例**：

```scala
// 在 Executor 端初始化数据库连接池
val bcInit = sc.broadcast(())  // 广播一个空 Unit

rdd.foreachPartition { iter =>
  // 利用广播变量的副作用初始化连接池
  bcInit.value  // 确保 Executor 加载了广播变量
  val conn = createConnectionPool()  // 每个 Executor 初始化一次
  iter.foreach { record =>
    conn.send(record)
  }
  conn.close()
}
```

### Q: 如何调试广播变量和累加器？

```scala
// 1. 查看广播变量大小
val bc = sc.broadcast(largeData)
println(s"Broadcast ID: ${bc.id}")  // 在 Spark UI 的 Storage 页可以看到

// 2. 使用 Spark Listener 监控
import org.apache.spark.scheduler.{SparkListener, SparkListenerTaskEnd}

sc.addSparkListener(new SparkListener {
  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    // 记录每个 Task 的累加器更新
    val metrics = taskEnd.taskMetrics
    // 可以通过 metrics.accumulatorUpdates 获取
  }
})

// 3. 累加器命名规范（便于在 Spark UI 中识别）
val acc1 = sc.longAccumulator("batch_01_error_count")
val acc2 = sc.longAccumulator("batch_01_total_count")
val acc3 = sc.longAccumulator("batch_01_null_count")
```

### Q: 广播变量和累加器的序列化问题

```scala
// ❌ Task not serializable 常见原因
class MyProcessor(val lookup: Map[String, Int]) extends Serializable {
  def process(rdd: RDD[(String, Int)]): RDD[(String, Int)] = {
    rdd.map { case (k, v) =>
      (k, v + lookup.getOrElse(k, 0))  // lookup 是整个 MyProcessor 实例被序列化
    }
  }
}

// ✅ 解决方法：只广播需要的部分
class MyProcessor(val lookup: Map[String, Int]) extends Serializable {
  @transient lazy val bcLookup: Broadcast[Map[String, Int]] = 
    SparkEnv.get.sparkContext.broadcast(lookup)
    
  def process(rdd: RDD[(String, Int)]): RDD[(String, Int)] = {
    rdd.map { case (k, v) =>
      (k, v + bcLookup.value.getOrElse(k, 0))
    }
  }
}
```

## 小结

| 概念 | 要点 |
|------|------|
| 广播变量 | 每个 Executor 存一份，Task 本地读取 |
| 广播 Join | 小表广播避免 Shuffle，解决倾斜 |
| 累加器 | 分布式只写变量，适合计数场景 |
| 累加器风险 | Transformation 中因重试不准确 |
| 共性原理 | 减少网络传输，利用本地性 |

**一句话总结**：广播变量是「Driver 写 → Executor 读」的只读共享变量，适合分发大尺寸维表；累加器是「Task 写 → Driver 读」的只写计数器，适合分布式场景下的聚合计数和监控。两者共同解决了分布式计算中「数据怎么过去」和「结果怎么回来」的核心问题。

> **面试最终提醒**：当面试官问「Spark 性能调优你做了哪些？」时，广播变量是必答项；当问「如何精确统计处理了多少条数据？」时，累加器是必答项。两者的共同关键词是——**减少不必要的网络传输**。
