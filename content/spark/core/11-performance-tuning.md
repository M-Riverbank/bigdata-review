# Spark Core — 性能调优

## 性能调优总览

Spark 的"快"是业界公认的——比 Hadoop MapReduce 快 10~100 倍。但这是建立在**正确调优**的前提上的。很多新手上来就抱怨"Spark 怎么比 MR 还慢"，十有八九是默认配置没动，代码写得随心所欲，最后全凭集群硬扛。

Spark 性能调优本质上是一个**系统工程**，不是调一个参数就能解决的。它涉及代码写法、内存分配、并行度设置、Shuffle 优化、数据分布等多个维度。就像开赛车，光踩油门不够，还得懂换挡时机和过弯路线。

我们把性能调优拆解为五个核心维度：

```
1. 代码优化       — 选择合适的算子、避免通用陷阱
2. 内存调优       — 合理配置内存、GC 优化
3. 并行度调优     — Task、分区、Executor 配置
4. Shuffle 调优   — 减少 Shuffle 数据、并行度
5. 数据倾斜       — 倾斜诊断与处理
```

> **面试点**：面试官最爱问的一个问题就是——"你的 Spark 任务跑得慢，你会从哪些角度去排查和优化？"标准答案就是先把这五个维度过一遍，从代码到资源再到数据分布，逐步缩小排查范围。

### 性能调优的黄金法则

在实际工作中，我总结了三条"黄金法则"，任何时候检查性能问题都先套用：

| 法则 | 说明 | 检查手段 |
|------|------|---------|
| **减少数据量** | 尽早过滤、尽早聚合、尽早序列化 | Spark UI Input 大小 vs 输出大小 |
| **减少 Shuffle** | Shuffle 是性能的第一杀手 | Stage 数、Shuffle Read/Write 量 |
| **减少序列化开销** | 跨网络传输的数据越小越好 | 序列化格式、缓存级别 |

这三条法则贯穿了所有优化手段。如果一条优化与这三条相悖，那很可能就是个伪优化。

### 性能问题排查的通用流程

当你接手一个慢任务时，别急着改参数。按以下步骤来：

```
Step 1: 看 Spark UI → Event Timeline、Stage 耗时
Step 2: 看 SQL 执行计划 → 是否有 Full Scan、BroadcastNestedLoopJoin
Step 3: 看 GC 时间 → 是否 > 10%
Step 4: 看数据倾斜 → 某个 Task 数据量远超其他 Task
Step 5: 看资源利用率 → CPU 是否跑满？内存是否够？
```

> **踩坑经验**：有一次我们团队的一个任务跑了 2 小时，大家都以为是数据量大。结果我一查 Spark UI，发现某个 Stage 的某个 Task 跑了 1 小时 50 分钟，其他 Task 几十秒就完了——典型的数据倾斜。改了一个 key 的哈希策略，任务降到 15 分钟。所以说，**调优的第一步永远是看 UI，不是改参数**。

## 代码优化

### 选择合适的算子

Spark 提供了丰富的算子 API，但不同的算子性能差异巨大。"选对算子"是代码优化的第一课，也是最容易见效的。

```scala
// ❌ 低效：先 collect 再处理
val data = rdd.collect()  // 全部拉到 Driver，可能 OOM
data.filter(_.length > 10).length

// ✅ 高效：利用 RDD 分布式处理
rdd.filter(_.length > 10).count()
```

上面这个例子看起来很基础，但在实际代码 review 中我见过太多次了。`collect()` 把全量数据拉到 Driver，然后只在 Driver 单机处理——RDD 的分布式优势完全废掉了。如果数据量稍大，直接 OOM。

| 场景 | 错误做法 | 正确做法 | 性能差异 |
|------|---------|---------|---------|
| 求满足条件的记录数 | `collect().filter(...).size` | `rdd.filter(...).count()` | 10x~100x |
| 分组后求和 | `groupByKey().mapValues(_.sum)` | `reduceByKey(_ + _)` | 2x~5x |
| 按 Key 去重后计数 | `groupByKey().mapValues(_.toSet.size)` | `distinct().countByKey()` | 3x~10x |

```scala
// ❌ 低效：groupByKey + 去重
rdd.groupByKey().mapValues(_.toSet.size)

// ✅ 高效：distinct + countByKey
rdd.distinct().countByKey()
```

> **面试点**：面试官问"`groupByKey` 和 `reduceByKey` 有什么区别？"不光要答"`reduceByKey` 有 Combiner"，还要说清楚 Combiner 减少了多少网络传输——假设 1 亿条数据有 1000 个 key，`groupByKey` 需要传输 1 亿条，而 `reduceByKey` 只需要传输 1000 条聚合结果，差了 10 万倍。

### 避免通用陷阱

工作中最常见的三个陷阱，每一个我都踩过，分享出来帮你避坑：

```scala
// 陷阱 1：在 foreach 中创建连接
rdd.foreach { record =>
  val conn = new JDBCConnection(url)  // 每条记录创建一个连接！
  conn.save(record)
  conn.close()
}
// ✅ 改为 foreachPartition
rdd.foreachPartition { iter =>
  val conn = new JDBCConnection(url)  // 每个分区一个连接
  iter.foreach(record => conn.save(record))
  conn.close()
}
```

> **踩坑经验**：第一个陷阱是"经典新手错误"。假如你有 1 亿条数据、200 个分区，`foreach` 会创建 1 亿个连接，数据库不被搞挂才怪。`foreachPartition` 只创建 200 个连接，差异一目了然。而且别忘了在 `finally` 里关闭连接，防止异常时连接泄漏。

```scala
// 陷阱 2：不必要的 Shuffle
rdd.map(x => (x.key, x.value)).groupByKey().mapValues(_.sum)
// ✅ reduceByKey 避免 Shuffle
rdd.map(x => (x.key, x.value)).reduceByKey(_ + _)
```

第二个陷阱是"无意识的 Shuffle"。`groupByKey` 会把所有 key 相同的记录通过网络拉到同一个分区，这个代价极其高昂。而 `reduceByKey` 在 Map 端先做一次 Combiner（类似于 MapReduce 的 Combiner），大大减少了网络传输。

| 算子 | Shuffle 数据量 | 适用场景 |
|------|---------------|---------|
| `groupByKey` | 全量数据 | 必须保留所有记录的场景（如排序后取 Top N） |
| `reduceByKey` | 聚合后数据 | 按 key 做聚合运算（sum、max、min） |
| `aggregateByKey` | 压缩后数据 | 需要自定义聚合逻辑，且可以分区预聚合 |
| `combineByKey` | 压缩后数据 | 更底层的聚合，类型可变的场景 |

```scala
// 陷阱 3：链式重复计算
val base = sc.textFile("hdfs://data")
val count1 = base.filter(predicate1).count()
val count2 = base.filter(predicate2).count()  // base 被重新读取
// ✅ 缓存公共 RDD
val base = sc.textFile("hdfs://data").cache()
```

第三个陷阱最难发现。因为 RDD 的 Lineage 机制，每次 Action 都会重新计算整个 DAG。上例中 `base` 被计算了两次，如果 `base` 的读取成本很高（如从 HDFS 读 1TB 数据），就白白浪费了一倍的 I/O 和计算时间。

> **踩坑经验**：有一次排查一个跑 3 小时的任务，发现同一份数据被读了 5 次——因为多个下游分析都引用了同一个原始 RDD 但没缓存。加了 `.cache()` 后，总时间降到 40 分钟。**缓存是你最便宜的优化手段**。

### 使用累加器替代 Action 的多次调用

```scala
// ❌ 低效：两次 Action
val cnt1 = rdd.filter(c1).count()
val cnt2 = rdd.filter(c2).count()

// ✅ 高效：一次 foreach + 累加器
val acc1 = sc.longAccumulator()
val acc2 = sc.longAccumulator()
rdd.foreach { v =>
  if (c1(v)) acc1.add(1)
  if (c2(v)) acc2.add(1)
}
```

**原理**：每一次 `count()` 都是一个 Action，会触发一次完整的 Job 提交——包括 DAG 调度、Task 分配、Stage 划分。如果需要统计多个条件，一次遍历打多个勾远比多次遍历高效。

| 方式 | Action 次数 | 扫描数据次数 | 性能 |
|------|------------|-------------|------|
| 多次 `count()` | N 次 | N 次 | 慢 |
| 单次 `foreach` + 累加器 | 1 次 | 1 次 | 快 |
| `map` + `cache` + 多次 `count` | N 次 | 1 次 + 缓存 | 中等但灵活 |

```scala
// 更多累计器使用场景
val nullCount = sc.longAccumulator()
val emptyCount = sc.longAccumulator()
val totalCount = sc.longAccumulator()

df.foreach { row =>
  totalCount.add(1)
  if (row.isNullAt(0)) nullCount.add(1)
  if (row.getString(1).isEmpty) emptyCount.add(1)
}

println(s"总记录：${totalCount.value}，空值：${nullCount.value}，空串：${emptyCount.value}")
```

### Broadcast 变量优化

除了累加器，`Broadcast` 变量也是一个容易被忽视的优化手段：

```scala
// ❌ 低效：在 map 中引用外部大变量
val lookupTable = loadLargeMapFromDB()  // 几百 MB
rdd.map { key =>
  lookupTable.getOrElse(key, "unknown")  // 每次 Task 都序列化一份
}

// ✅ 高效：Broadcast 变量，每个 Executor 只存一份
val lookupTableBC = sc.broadcast(loadLargeMapFromDB())
rdd.map { key =>
  lookupTableBC.value.getOrElse(key, "unknown")
}
```

> **原理说明**：没有 Broadcast 时，`lookupTable` 被闭包捕获，每个 Task 序列化一份——如果有 1000 个 Task，就有 1000 份拷贝。Broadcast 后，每个 Executor 只存一份（用高效的 P2P 协议传输），极大节省内存和网络。

### 代码优化四点总结

- 能用集群算的别拉到 Driver 算
- 能用 `reduceByKey` 的别用 `groupByKey`
- 能用一次 Action 完成的别用多次
- 能 Broadcast 的大变量别闭包捕获

## 并行度调优

### 原则

并行度是 Spark 性能调优中最"立竿见影"的参数——调对了，CPU 跑满；调错了，集群一半在摸鱼。

```scala
// 核心理念：
// Task 数量 = 分区数
// 建议：每个 CPU 核分配 2~4 个 Task

// 如果一个 Executor 有 4 核：
// Executor 并行度 = 4（同时运行 4 个 Task）
// 分区数建议 = 4 × 3 = 12（让每个核工作不闲置）
```

为什么建议乘 2~4 而不是 1:1？原因有三：

| 原因 | 说明 |
|------|------|
| **数据倾斜缓冲** | 某些 Task 处理快、某些慢，多的分区能平滑"长尾" |
| **调度开销掩盖** | Task 启动有开销，多一些分区让计算时间掩盖调度时间 |
| **资源竞争缓解** | I/O 密集型 Task 会等待磁盘/网络，此时 CPU 可以切到其他 Task |

**什么情况不需要乘倍？** 如果你的 Task 完全是纯 CPU 计算（如复杂数学运算），1:1 就够，多了反而增加调度开销。实际工作中绝大多数场景是 I/O 混合型，2~4 倍是最佳区间。

### 调整并行度

```scala
// 全局默认
spark.conf.set("spark.default.parallelism", "200")  // RDD
spark.conf.set("spark.sql.shuffle.partitions", "200") // SQL

// 算子级别
rdd.reduceByKey(_ + _, 400)  // 4 倍默认并行度
df.repartition(400)
df.coalesce(100)
```

> **踩坑经验**：`spark.default.parallelism` 和 `spark.sql.shuffle.partitions` 是两个不同的参数！前者控制 RDD 操作的默认分区数，后者控制 Spark SQL 执行 Shuffle 时的分区数。很多人只设置了第一个，结果跑 SQL 查询时 Shuffle 分区数还是 200。**两个都得设**，或者统一用 `spark.sql.shuffle.partitions` 因为现在大家都在用 Dataset/DataFrame API。

| 方法 | 效果 | Shuffle 开销 | 适用场景 |
|------|------|-------------|---------|
| `repartition(N)` | 增加或减少分区 | 有（全量 Shuffle） | 分区数太少时增加 |
| `coalesce(N)` | 只减少不分区 | 无（不跨节点） | 分区数太多时合并 |
| `repartitionByRange(col)` | 按范围分区 | 有（但排序开销大） | 需要数据有序的分区 |

```scala
// coalesce vs repartition 的陷阱
val rdd = sc.parallelize(1 to 10000, 100)

// coalesce 只是合并分区，不产生 Shuffle（但不一定均匀）
rdd.coalesce(10)  // 只是把相邻分区合并，数据可能不均衡

// repartition 重新洗牌，数据更均衡
rdd.repartition(10)  // 产生 Shuffle，但数据均匀
```

> **面试点**：面试官问"`coalesce` 和 `repartition` 的区别"——标准答案是 `coalesce` 通过合并减少分区数，默认不会 Shuffle；`repartition` 会触发全量 Shuffle。进阶答案是 `coalesce` 如果合并比例太大（如从 1000 个分区合并到 10 个），建议先 `coalesce(100, shuffle=true)` 触发一次 Shuffle 让数据均匀，再 `coalesce(10)` 合并，这样兼顾了均匀性和性能。

### 并行度判断标准

```scala
// 一个 Task 处理的数据量建议范围
// 太小 → 调度开销占比大（10 万个 Task 调度耗时就很可观了）
// 太大 → 并行度不足、GC 压力大、失败恢复成本高

// 核心指标：每个 Task 处理 100MB ~ 200MB 数据
```

```
分区大小建议：100MB ~ 200MB / 分区

举例：
- 输入数据：1TB
- 目标分区大小：128MB
- 建议分区数：1TB / 128MB = ~8000
- 集群总核数：200
- Task 数：8000 / 200 = 40 轮 Task

经验公式：
分区数 = max(输入分区数, shuffle.partitions)
每个 Task 处理时间建议 100ms ~ 10s
时间过短 → 分区太多，调度开销大
时间过长 → 分区太少，并行度不足
```

150MB 这个"黄金分割点"来自哪里？其实是 HDFS 块大小的默认值（128MB / 256MB）。Spark 的分区策略沿用了这个设计，因为让每个 Task 处理一个 HDFS 块大小的数据，可以最大化数据本地性（数据就在本机，不需要网络读取）。

**不同场景的推荐分区大小**：

| 场景 | 推荐分区大小 | 原因 |
|------|-------------|------|
| 纯文本读取（CSV/JSON） | 128MB ~ 256MB | 读取快，I/O 为主 |
| Parquet/ORC 列式存储 | 256MB ~ 512MB | 压缩率高，有效数据量小 |
| Shuffle 中间结果 | 100MB ~ 200MB | 需要在网络传输，不宜过大 |
| 有缓存 + 反复计算 | 200MB ~ 400MB | 减少缓存对象数量，降低 GC |
| 倾斜严重 | 500MB ~ 1GB | 需要手动调大分区数消除倾斜 |

> **踩坑经验**：别把分区调得太多！有一次我们把一个 10GB 的数据调到 10000 个分区（每个分区才 1MB），结果每个 Task 几毫秒就跑完了，调度开销反而占了 80%。Spark UI 上一大片灰色（调度等待时间），绿色（实际计算时间）几乎看不见。最终调到 200 个分区（每个 ~50MB），总时间从 3 分钟降到 40 秒。

### 并行度调优小结

- 每个 CPU 核分配 2~4 个 Task 是最常用的经验值
- 每个 Task 处理 100~200MB 数据是目标
- **宁可分区偏多，也不要偏少**——多分区可以 coalesce，少分区只能 repartition（全量 Shuffle）
- 观察 Spark UI：如果并行度不够，你会看到某些 Task 处理时间异常长

## 资源调优

### Executor 配置

资源调优的主角就是 Executor——Spark 的"工人"。工人多了，干得快；工人配置好了，干得顺。

```scala
// 推荐配置示例（YARN 集群）
spark.executor.memory = 8g     // 每 Executor 内存
spark.executor.cores = 4       // 每 Executor CPU 核数
spark.executor.instances = 50  // Executor 数量
spark.driver.memory = 4g       // Driver 内存

// 总资源 = 50 × 8g = 400g 内存
// 并行度 = 50 × 4 = 200 并发 Task
```

> **面试点**：面试官经常问"如何确定一个合理的 Executor 配置？"这是一个开放性问题，没有标准答案，但好的回答应该展示出对资源分配的理解——"我会先看集群每台机器的配置（如 32 核 128GB），然后考虑给操作系统和 YARN NM 预留一部分（通常 1 核 8GB），剩下的分配给 Executor。如果每台机器配 2 个 Executor，每个 12 核 48GB——等等，这样每个 Executor 核数太多了（>5），会导致 HDFS 写入竞争。所以我会调整为每台 4 个 Executor，每个 5 核 25GB。"

### 配置原则

```
1. 每个 Executor 内存 4~32g（过大：GC 时间长）
2. 每个 Executor 核数 2~5（过多：HDFS 写入竞争）
3. Executor 数量由队列资源决定
4. Driver 内存取决于 collect 数据量

经验公式：
总核心数 = num-executors × executor-cores
总内存 = num-executors × executor-memory
shuffle.partitions ≈ cores × 2~3
```

| 机器配置 | Executor 方案 1 | Executor 方案 2 | 推荐 |
|---------|----------------|----------------|------|
| 16 核 64GB | 2 Executor × 7c × 28g | **4 Executor × 3c × 14g** | 方案 2（核数适中） |
| 32 核 128GB | 4 Executor × 7c × 28g | **6 Executor × 5c × 18g** | 方案 2（更均衡） |
| 48 核 256GB | 6 Executor × 7c × 36g | **8 Executor × 5c × 28g** | 方案 2（避免大核大内存） |

为什么我更推荐方案 2？核心是**避免"大 Executor"陷阱**：

```scala
// 大 Executor（7c 36g）的问题：
// 1. GC 暂停时间长（36G 堆 → Full GC 可能几秒甚至十几秒）
// 2. HDFS 写入竞争（7 个 Task 同时写一个 HDFS 文件块）
// 3. 失败恢复成本高（一个 Executor 挂掉损失 7c 的计算能力）

// 小 Executor（5c 18g）的好处：
// 1. GC 更快更可控
// 2. 失败影响面小
// 3. 弹性更好（YARN 更容易调度）
```

> **踩坑经验**：有过一次经历——把 Executor 配到了 64GB，想着"内存越大越好"。结果 Full GC 频繁触发，单次 Full GC 停了几十秒，Executor 心跳超时被 YARN 杀掉，任务不断重试，跑得反而比 16GB 时还慢。所以**内存不是越大越好**，要适配 GC 能力。

### YARN 资源分配

```scala
// YARN 容器内存计算
// Executor 实际占用 = executor-memory + spark.yarn.executor.memoryOverhead
// memoryOverhead = max(384MB, 10% × executor-memory)

// 举例：executor-memory=8g
// memoryOverhead = max(384, 819) = 819MB
// YARN 申请 = 8192 + 819 = 9011MB ≈ 9g
```

这个 `memoryOverhead` 是一个容易踩坑的点。它用于存放 JVM 之外的额外内存消耗——如 Spark 内部数据结构、线程栈、NIO Buffer、Direct Memory 等。很多人在 YARN 上申请了 8GB Executor，实际启动时 YARN 显示申请了 ~9GB，困惑不解。上面这个计算就是原因。

```scala
// 更精细的配置
// 可以手动设置 memoryOverhead（覆盖默认比例）
spark.yarn.executor.memoryOverhead = 1024  // 单位 MB

// 如果 Executor 用到大量 off-heap 内存（如 Arrow、TensorFlow 集成）
// 需要调大 overhead
spark.executor.memory = 16g
spark.yarn.executor.memoryOverhead = 4096  // 4G overhead，因为 off-heap 需求大
```

### 资源调优的"底线思维"

- **最小配置**：一个 Executor 至少 4GB，少于 4GB 连 Spark 内部数据结构都放不下
- **最大 Executor 核数**：建议不超过 5 个，超过 5 个 HDFS 写入竞争剧烈
- **最大 Executor 内存**：建议不超过 32GB，超过 32GB Full GC 难以接受
- **Driver 内存下限**：如果用到 `collect`，Driver 至少要能装下一个分区的数据

## GC 调优

### 常见 GC 问题

GC（垃圾回收）是 Spark 性能的"隐形杀手"。你不盯着它看的时候，它悄悄偷你的时间；你盯着它看了，又拿它没什么好办法。

```
GC 长暂停的症状：
1. Executor 心跳超时 → Driver 标记 Executor 死亡
2. Task 处理时间远大于实际计算时间
3. Spark UI 中看到 JVM GC 时间占比 > 10%
```

| GC 问题类型 | 症状 | 常见原因 |
|------------|------|---------|
| Minor GC 频繁 | CPU 使用率高但吞吐低 | 对象创建过多，Eden 区太小 |
| Full GC 长暂停 | Executor 心跳丢失 | 老年代满了，对象无法回收 |
| G1GC Mixed GC 慢 | 吞吐量周期性下降 | 分区数太多，标记耗时 |

如何通过 Spark UI 判断 GC 有问题：

```scala
// 在 Spark UI 的 Executors 页面
// 看 GC Time 列
// 正常：GC Time < 总计算时间的 5%
// 警告：GC Time 占比 5%~10%
// 严重：GC Time 占比 > 10%
```

### GC 优化策略

```scala
// 1. 使用 G1GC（推荐，Spark 3.x 默认）
spark.executor.extraJavaOptions = "-XX:+UseG1GC"
```

为什么推荐 G1GC？

| GC 算法 | 适合场景 | 暂停时间 | 吞吐量 |
|---------|---------|---------|--------|
| Serial GC | 单核小内存 | 长 | 低 |
| Parallel GC（ParNew） | 多核，追求吞吐 | 较长 | 高 |
| **G1GC** | 大堆内存（>4GB） | **可控** | 高 |
| ZGC（JDK 11+） | 超大堆（>100GB） | 极短 | 中 |

```scala
// G1GC 调优参数（Spark 3.x+）
spark.executor.extraJavaOptions = """
  -XX:+UseG1GC
  -XX:MaxGCPauseMillis=200       // 目标每次 GC 暂停不超过 200ms
  -XX:InitiatingHeapOccupancyPercent=45  // 当堆占用 45% 时启动并发标记
  -XX:ConcGCThreads=4            // 并发 GC 线程数
  -XX:G1HeapRegionSize=16m       // G1 Region 大小（默认会根据堆大小自动计算）
"""
```

> **踩坑经验**：G1GC 不是银弹！如果你的 Executor 只有 2GB，用 G1GC 反而不如用 Parallel GC。因为 G1GC 需要划分 Region、维护 RSet（Remembered Set），这些都有额外开销。小堆内存（<4GB）建议用 Parallel GC，大堆建议用 G1GC。

```scala
// 2. 减小缓存对象体积
// 用 MEMORY_ONLY_SER 代替 MEMORY_ONLY
rdd.persist(StorageLevel.MEMORY_ONLY_SER)
```

| 存储级别 | 空间占用 | CPU 开销 | GC 压力 | 推荐场景 |
|---------|---------|---------|---------|---------|
| `MEMORY_ONLY` | 大（原始对象） | 无 | 大 | 小数据量 |
| `MEMORY_ONLY_SER` | 小（序列化） | 序列化/反序列化 | 小 | **大多数场景** |
| `MEMORY_AND_DISK_SER` | 小 + 磁盘溢出 | 中 | 小 | 内存不足 |
| `DISK_ONLY` | 磁盘 | 大 | 几乎无 | 超大数据，内存放不下 |

> **面试点**：面试官问"`MEMORY_ONLY` 和 `MEMORY_ONLY_SER` 选哪个？"——答案不是简单选 SER。如果你的数据要反复计算多次（迭代算法），序列化/反序列化开销可能超过 GC 节省的时间。建议先压测试一下，如果 GC 时间是瓶颈就选 SER，如果是 CPU 计算是瓶颈就选 `MEMORY_ONLY`。

```scala
// 3. 减少不必要的对象创建
rdd.map { record =>
  // ❌ 每次创建新对象
  val result = new MutableObject()
  // ✅ 复用可变对象（但不安全！）
  // 推荐：mapPartitions 内统一管理
}
```

对象创建在 Java/Scala 中无处不在，但每条记录都 new 一个临时对象，在 10 亿条数据下就是 10 亿次 GC 压力。更好的做法：

```scala
// ✅ 推荐：在 mapPartitions 中统一管理对象
rdd.mapPartitions { iter =>
  // 只创建一次
  val buffer = new StringBuilder(1024)
  iter.map { record =>
    buffer.clear()
    buffer.append(record.field1)
    buffer.append(",")
    buffer.append(record.field2)
    buffer.toString
  }
}
```

**注意**：对象复用要小心！如果引用了可变对象并在后续使用，会出现"数据全部变成最后一条"的经典 bug。只在线性处理的管道中使用。

### 统一内存管理（Spark 1.6+）

```
Spark 堆内存划分（以 8GB Executor 为例）：
┌────────────────────────────────────────┐
│ Reserved Memory (300MB)                 │  ─ 系统保留，不可配置
├────────────────────────────────────────┤
│ User Memory (20% × (8GB - 300MB)       │  ─ UDF 用户使用，约 1.5GB
│   = ~1.5GB)                            │
├────────────────────────────────────────┤
│ Spark Memory (80% × (8GB - 300MB))     │
│   ┌─────────────────┬────────────────┐  │
│   │ Storage (50%)   │ Execution (50%)│  │  ─ 可以互相借用
│   │  ≈ 3GB          │  ≈ 3GB         │  │
│   └─────────────────┴────────────────┘  │
└────────────────────────────────────────┘
```

关键配置参数：

```scala
spark.memory.fraction = 0.8       // Spark 可使用比例（默认 0.6）
spark.memory.storageFraction = 0.5 // Storage 初始比例（默认 0.5）

// 调大 memory.fraction 可以给缓存更多空间
// 但用户代码可用的 User Memory 会减少
```

### GC 调优的核心要点

- G1GC 适合大堆（>4GB），Parallel GC 适合小堆
- 序列化存储（`MEMORY_ONLY_SER`）是降低 GC 压力的第一选择
- `mapPartitions` 比 `map` 更适合管理对象生命周期
- GC 时间占比 > 10% 就说明需要优化了

## 序列化调优

### Kryo 序列化

序列化在 Spark 中无处不在：Shuffle 写磁盘、跨网络传输、缓存到内存、Task 分发……序列化的速度和大小直接影响每一步的性能。

```scala
// Spark 默认使用 Java 序列化（慢，体积大）
// 推荐使用 Kryo 序列化（快 10x，体积小 3x）

spark.conf.set("spark.serializer",
  "org.apache.spark.serializer.KryoSerializer")

// 注册自定义类（不注册也能用，但效率低）
spark.conf.set("spark.kryo.registrationRequired", "true")

// 注册类
class MyKryoRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo): Unit = {
    kryo.register(classOf[MyClass])
  }
}
spark.conf.set("spark.kryo.registrator",
  "com.example.MyKryoRegistrator")
```

| 对比指标 | Java 序列化 | Kryo 序列化 | 差距 |
|---------|------------|------------|------|
| 序列化速度 | ~100MB/s | ~1000MB/s | **10x** |
| 反序列化速度 | ~120MB/s | ~800MB/s | **~7x** |
| 对象体积 | 100%（基准） | 20%~30% | **小 3~5 倍** |
| 是否注册 | 不需要 | 推荐注册 | 注册后更优 |

**为什么 Kryo 更快？**
- Java 序列化使用反射遍历对象图，记录完整的类结构信息+数据
- Kryo 通过 ID 映射类信息，只记录 ID 和原始数据
- Kryo 还做了零拷贝优化、直接操作字节缓冲区等

```scala
// 不注册的自定义类（Kryo 也能用，但效率下降）
// 每次序列化时，Kryo 需要写入完整的类名（如 "com.example.MyVeryLongClassName"）
// 注册后，Kryo 只写一个整数 ID（如 42），节省大量空间

// 注册时常见陷阱
spark.conf.set("spark.kryo.registrationRequired", "true")
// 如果没有注册所有用到的类，会报错：
// Caused by: com.esotericsoftware.kryo.KryoException: Class is not registered:
// com.example.SomeClass
```

> **踩坑经验**：`registrationRequired = true` 是一个双刃剑。它保证性能最优，但当你用第三方库（如 `org.apache.spark.sql.types.StructType`）时，需要把所有涉及的类都注册。如果你不想折腾，可以不设这个参数（默认 false），Kryo 仍然可用，只是当遇到未注册的类时会退化为写入全类名，性能略差。**建议：生产环境注册自己的类，第三方类不强制注册**。

```scala
// Kryo 对 Scala 类的支持
// 注册 Scala 集合类
kryo.register(classOf[scala.collection.immutable.::[_]])
kryo.register(classOf[scala.collection.immutable.Nil$])

// 对于复杂嵌套结构，考虑使用 @throws 注解或自定义 Serializer
```

### 何时序列化？

```scala
// 场景 1：Shuffle 写（自动发生）
rdd.reduceByKey(_ + _)  // map 端输出必须序列化后写入磁盘

// 场景 2：缓存（由存储级别决定）
rdd.persist(StorageLevel.MEMORY_ONLY_SER)  // 序列化后缓存

// 场景 3：跨网络传输（自动发生）
// Executor 之间、Driver 与 Executor 之间的数据传输
```

**序列化最佳实践**：

1. **全局启用 Kryo**：`spark.serializer = KryoSerializer`——这是性价比最高的配置，一行代码换 3~10 倍性能提升
2. **缓存优先用序列化级别**：`MEMORY_ONLY_SER` 比 `MEMORY_ONLY` 省 3 倍空间，GC 压力也小
3. **复杂对象考虑自定义序列化器**：如果你有大量复杂嵌套对象，可以写自定义 Kryo Serializer 达到最优
4. **不要序列化 Tuple/Case Class 太多层**：深层嵌套的序列化/反序列化开销仍然可观

### 序列化调优小结

- 一行配置 `spark.serializer KryoSerializer` 就能带来显著的性能提升
- Kryo 注册不是必须的，但推荐注册以获得最佳性能
- 序列化不仅影响速度，还影响内存占用和 GC

## 数据本地性调优

"数据本地性"是 Spark 做调度时的一个重要概念——它指 **Task 被调度到离数据最近的计算节点**。如果能做到"数据不动代码动"，那就省去了网络传输的巨额开销。

Spark 的数据本地性分为五个级别（从最优到最差）：

| 级别 | 含义 | 延迟 |
|------|------|------|
| `PROCESS_LOCAL` | Task 与数据在同一 JVM 进程 | 0 |
| `NODE_LOCAL` | Task 与数据在同一节点（不同进程） | 低 |
| `RACK_LOCAL` | Task 与数据在同一机架 | 中 |
| `ANY` | 跨数据中心 | 高 |

```scala
// 如果 Task 大量处于 RACK_LOCAL 或 ANY
// 说明数据本地性差，需要调整

// 方案 1：增大等待时间
spark.locality.wait = 5s  // 默认 3s
```

增大 `locality.wait` 可以让 Spark 多等待一会儿，期望得到更好的本地性。但这是"用时间换空间"——等待本身也有开销。

```scala
// spark.locality.wait 的细粒度控制
spark.locality.wait.process = 3s   // PROCESS_LOCAL 等待时间
spark.locality.wait.node = 3s      // NODE_LOCAL 等待时间
spark.locality.wait.rack = 3s      // RACK_LOCAL 等待时间
```

调整策略：

| 场景 | 建议 | 原因 |
|------|------|------|
| 计算密集 > I/O | 减小 `locality.wait`（如 1s） | 计算时间长，网络传输占比小，不值得等 |
| I/O 密集 > 计算 | 增大 `locality.wait`（如 5~10s） | 数据量巨大，网络传输是瓶颈 |
| 集群负载高 | 适当增大 | 资源竞争，等一等可能拿到本地资源 |

```scala
// 方案 2：增加 Executor（提高 Task 与数据同机的概率）
// 每台机器更多 Executor → 更多 Task 在同一台机器 → 更好的本地性

// 方案 3：调整数据分区（coalesce）
// 把数据合并到更少的分区，减少跨节点读取
```

> **踩坑经验**：有一次我们一个 200 节点集群上的任务，大量 Task 处于 `RACK_LOCAL` 级别。各种调参数都没用。后来发现是一个运维问题——数据和计算集群的 YARN NodeManager 配置不一致，导致 Spark 认为"数据在这里，但资源不在"。最终修了 YARN 配置，`PROCESS_LOCAL` 比例从 30% 提升到 95%。

**查看本地性状态**：在 Spark UI 的 Stages 页面——点进某个 Stage，看 Locality Level 列。如果大量显示 `RACK_LOCAL` 或 `ANY`，说明本地性差，需要排查。

### 数据本地性优化实战

```scala
// 检查当前任务的本地性分布
// Spark UI → Stages → 点击某个 Stage → Locality Level Summary

// 如果 PROCESS_LOCAL + NODE_LOCAL < 80%，需要关注

// 快速优化：
// 1. 确认 HDFS 副本因子 >= 2（让数据有更多副本，更容易本地读取）
// 2. 确认 HDFS 和 YARN 节点集一致
// 3. 增大 spark.locality.wait（但要权衡等待成本）
// 4. 使用 Spark 3.x 的 Adaptive Query Execution (AQE)
```

## 面试高频考点

### Q: Spark 中 OOM 如何排查？

OOM 是 Spark 面试中最高频的问题之一。首先要分清是 **Driver OOM** 还是 **Executor OOM**，两者的原因和解决方案完全不同。

#### Driver OOM 排查思路

| 可能原因 | 排查方法 | 解决方案 |
|---------|---------|---------|
| `collect()` 拉取过多数据 | 查看代码中是否有 `collect()` | 改为 `take(N)` 或分批 collect |
| `collectAsMap()` 结果太大 | 查看 Map 大小 | 使用 `countByKey` 或 `aggregateByKey` |
| Spark UI 累积元数据 | 长时间运行任务 | 增大 `spark.driver.memory` |
| Broadcast 变量太大 | 查看 Broadcast 大小 | 减少 Broadcast 内容或改用外部存储 |

#### Executor OOM 排查思路

```
Executor OOM 排查五步法：

1. 检查是哪个 Stage OOM
   → Spark UI 中找到失败的 Stage

2. 看该 Stage 的分区数和数据量
   → 如果分区数太少，每个分区数据量太大 → 增大分区数
   
3. 看是否发生数据倾斜
   → 如果某个 Task 数据量远大于其他 Task → 先处理数据倾斜

4. 如果以上都正常
   → 检查 UDF 中是否缓存了大量数据（如全局 Map）
   → 检查是否有大对象未释放

5. 还是不行
   → 增大 Executor 内存
   → 改用 MEMORY_AND_DISK 存储级别（溢出到磁盘）
```

```scala
// 常见 OOM 场景与修复

// 场景 1：groupByKey 导致单个 Key 的数据量太大
// ❌ 危险
rdd.groupByKey().mapValues(_.sum)
// ✅ 安全
rdd.reduceByKey(_ + _)

// 场景 2：join 导致膨胀
// ❌ join 后数据可能膨胀数倍
val result = bigRdd.join(anotherBigRdd)
// ✅ 先过滤再 join
val filtered = bigRdd.filter(condition)
val result = filtered.join(anotherBigRdd)

// 场景 3：UDF 内缓存了大对象
rdd.map { record =>
  // ❌ 每次 Task 都加载一次大资源
  val model = loadLargeMLModel()  // 几百 MB
  model.predict(record.features)
}
// ✅ 在 mapPartitions 中只加载一次
rdd.mapPartitions { iter =>
  val model = loadLargeMLModel()
  iter.map(record => model.predict(record.features))
}
```

### Q: 如何确定合理的分区数？

这是一个"既要又要"的问题——分区数太少，并行度不够；分区数太多，调度开销大。面试官想听的是**权衡思维**。

**公式**：`分区数 = 数据总量 / 期望单个分区大小`

建议单个分区 100~200MB。例如 1TB 数据 → 8000~10000 个分区。同时参考集群总核数，让分区数 ≈ 核数 × 2~3。

但实际中还有两个重要限制条件：

```scala
// 限制 1：Task 创建开销
// 每个 Task 有固定的序列化、调度、启动开销（约 5~50ms）
// 如果 Task 处理时间 < 100ms，调度开销占比就太大了

// 限制 2：失败恢复成本
// 如果分区过大，Task 处理时间长，失败重试的成本也高
// 一个处理 1 小时的 Task 如果失败，重做就是 1 小时

// 实际判断方法：
// 观察 Spark UI 中每个 Task 的处理时间
// 如果大部分 Task 在 1s 以下 → 分区太多
// 如果大部分 Task 在 10s 以上 → 分区太少
// 理想：Task 时间在 1~10s 之间
```

### Q: Kryo 序列化比 Java 序列化好多少？

Kryo 比 Java 序列化快 10 倍，体积小 3~5 倍。但 Kryo 需要注册类（不注册也能用，但效率下降），而且不是所有 Java 对象都支持 Kryo 序列化。

**进阶回答**：Kryo 的优势主要体现在 Shuffle 和 Cache 阶段——Shuffle 时数据要落盘和网络传输，序列化后体积越小、速度越快，总体性能提升越明显。但在纯计算场景下，序列化只占很小一部分，Kryo 的优势不大。

### Q: Spark 任务你觉得最值得做的一个优化是什么？

**参考答案**："我会选 `MEMORY_ONLY → MEMORY_ONLY_SER` 加上 Kryo 序列化。一行配置改缓存级别、一行配置改序列化器，成本几乎为零，但带来的收益是 3~10 倍的序列化速度提升和 3 倍的内存节省。**性价比最高的两个优化**。"

### Q: Spark UI 中你怎么判断一个任务需要优化？

```
从 Spark UI 中找出"问题信号"：
1. Stage 耗时不均衡 → 数据倾斜
2. GC Time > 10% → GC 需要调优
3. Shuffle Read/Write 量异常大 → Shuffle 过多
4. Scheduling Delay 占比大 → 资源不够
5. Task 被反复失败重试 → 数据或代码有问题
6. 大量 Task 在 RACK_LOCAL 或 ANY → 本地性差
```

## 小结

| 维度 | 关键优化手段 | 最直接的配置/改动 |
|------|-------------|-----------------|
| 代码 | 选对算子、避免重复计算、`foreachPartition` | `cache()`、`reduceByKey`替代`groupByKey` |
| 并行度 | 分区 100~200MB、cores × 2~3 | `spark.sql.shuffle.partitions` |
| 内存 | 统一内存管理、`MEMORY_ONLY_SER` | `spark.memory.fraction` |
| GC | G1GC、减少对象创建 | `-XX:+UseG1GC` |
| 序列化 | Kryo 替代 Java 序列化 | `spark.serializer KryoSerializer` |
| 本地性 | 调整 `locality.wait`、增 Executor | `spark.locality.wait` |
| Shuffle | 预聚合、Map Join | `reduceByKey`、Broadcast Hash Join |

### 面试官最爱问的"性能优化"套路

> **面试点**：最后送你一个面试时的"万能回答框架"——当面试官问如何优化 Spark 任务时：
>
> 1. **先说方法论**：我会先看 Spark UI，确认瓶颈在哪里（CPU、内存、网络、GC？）
> 2. **从头排查**：先看 GC 时间是否正常 → 再看 Task 分布是否均匀 → 再看 Shuffle 量 → 最后看资源配置
> 3. **分情况讨论**：如果是数据倾斜就做加盐或两阶段聚合；如果是 GC 问题就调序列化和缓存级别；如果是并行度问题就调分区数
> 4. **给出效果**：每次优化后都要对比 before/after，用数据说话
>
> 记住——**没有万能配置，只有万能思路**。理解原理，比背参数重要 100 倍。
