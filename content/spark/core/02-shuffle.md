# Spark Shuffle 原理与调优

## Shuffle 是什么

Shuffle 是分布式计算中将数据按 key 在各节点间重新分配的过程——是所有大数据引擎中**最昂贵的操作**。

```
没有 Shuffle:                         有 Shuffle:
┌──────────┐  ┌──────────┐           ┌──────────┐  ┌──────────┐
│ Node A   │  │ Node B   │           │ Node A   │  │ Node B   │
│ [1,a]    │  │ [2,b]    │           │ [1,a]    │  │ [2,b]    │
│ [1,c]    │  │ [2,d]    │           │ [1,c]    │  │ [2,d]    │
└──────────┘  └──────────┘           └─────┬────┘  └────┬─────┘
        直接计算                             │            │
                                            │  网络传输    │
                                       ┌────▼──┐    ┌───▼─────┐
                                       │ Node A│    │ Node B  │
                                       │ [1,a] │    │ [2,b]   │
                                       │ [1,c] │    │ [2,d]   │
                                       └───────┘    └─────────┘
```

> 涉及 Shuffle 的算子：`reduceByKey`、`groupByKey`、`join`、`repartition`、`distinct`、`sortByKey`

## Shuffle 演化

### 1. HashShuffle（Spark 1.2 前，已淘汰）

```
问题：每个 Map Task 为每个 Reducer 创建独立文件
M 个 Map Task × R 个 Reducer = M × R 个文件

M=100, R=100 → 10000个小文件 → 磁盘IO爆炸
```

### 2. SortShuffle（当前默认）

```
过程：
1. Map 端写入内存缓冲区
2. 缓冲区满后，先按 partitionId 排序，可能再按 key 排序
3. 溢写到磁盘（一个文件 + 索引文件）
4. 所有溢写文件合并（Merge）
5. Reduce 端按索引读取对应段

每 Map Task 只产生 2 个文件（data + index）!
```

### 3. UnsafeShuffle / TungstenSortShuffle

```scala
// 启用条件：
// 1. 没有聚合操作（只是重新分区）
// 2. 序列化后的记录支持排序
// 3. 分区数 < 16777216

// 直接操作序列化后的二进制数据，无需反序列化
```

### 4. BypassMergeSortShuffle

```scala
// 条件：分区数 < spark.shuffle.sort.bypassMergeThreshold（默认 200）
//       且没有 Map 端聚合

// 为每个分区创建一个文件，最后合并
// 适用于分区少 + 无聚合的场景
```

## Shuffle 写流程详解

```
Map Task Shuffle Write:

1. 数据写入 ShuffleExternalSorter
   └→ 积累到一定量 → spill 到磁盘
      ├→ 按 (partitionId, key) 排序
      └→ 每个 spill 一个文件

2. spill 文件之间 merge
   └→ 最终合并为一个 sorted 文件
      ├→ data file: [partition0 records][partition1 records]...
      └→ index file: 记录每个 partition 的 offset
```

## Shuffle 读流程详解

```
Reduce Task Shuffle Read:

1. 从 MapOutputTracker 获取所需 block 位置

2. 对每个 Map Task 的输出：
   ├→ 远程拉取 → fetch 线程池（默认 5 个线程）
   ├→ 本地读取 → 直接读文件
   └→ 放入内存 (reduce 端缓冲区)

3. 内存满了 → 溢写到磁盘 (ExternalSorter)

4. 所有数据拉取完后 → merge + sort → 给用户算子
```

## 关键配置参数

### Map 端

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.shuffle.file.buffer` | 32K | Map 端写缓冲区大小 |
| `spark.shuffle.spill.batchSize` | 10000 | 每次 spill 的记录数 |

### Reduce 端

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.reducer.maxSizeInFlight` | 48M | 每个 reduce task 同时拉取的数据量上限 |
| `spark.shuffle.io.maxRetries` | 3 | 拉取失败重试次数 |
| `spark.shuffle.io.retryWait` | 5s | 重试等待时间 |

### 内存

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.shuffle.memoryFraction` | 0.2 | Shuffle 使用的执行内存占比 |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | bypass 机制的分区阈值 |

## 数据倾斜处理（面试必考！）

### 倾斜诊断

```scala
// 在 Spark UI 中查看：
// 1. Stage 详情页 → 各 Task 执行时间
//    - 大部分 Task 很快（秒级）→ 个别 Task 很慢（分钟级）↔ 数据倾斜
// 2. SQL 页 → Join/GroupBy 算子
//    - 查看每个 partition 的数据量

// 代码方式定位
df.groupBy("key").count().orderBy(desc("count")).show(20)
```

### 解决方案 1：两阶段聚合（Key 加盐）

```scala
val skewedRDD: RDD[(String, Long)] = ...

// 第一轮：加随机前缀聚合（每个 key 被拆分）
val saltRDD = skewedRDD.map { case (k, v) =>
  val salt = Random.nextInt(100)
  (s"${salt}_$k", v)
}.reduceByKey(_ + _)

// 第二轮：去掉前缀再聚合
val result = saltRDD.map { case (sk, v) =>
  val key = sk.substring(sk.indexOf("_") + 1)
  (key, v)
}.reduceByKey(_ + _)
```

### 解决方案 2：Map Join

```scala
// 小表 < broadcast 阈值（默认 10MB）
val smallDF = spark.read.parquet("dim_table").cache()

import org.apache.spark.sql.functions.broadcast
largeDF.join(broadcast(smallDF), "key")  // 显式 broadcast hint
```

### 解决方案 3：倾斜 Join

```scala
// 识别倾斜的 key
val skewedKeys = df.groupBy("key").count()
  .filter(col("count") > threshold)
  .select("key").collect().map(_.getAs[String]("key"))

// 对倾斜 key 加盐
val skewedDF = df.filter(col("key").isin(skewedKeys: _*))
  .withColumn("salt_key", concat(col("key"), lit("_"), rand(100).cast("int")))

val nonSkewedDF = df.filter(!col("key").isin(skewedKeys: _*))
// 对倾斜部分做加盐 Join，常规部分做普通 Join，最后 union
```

### 解决方案 4：调整并行度

```scala
// 第一种：spark.default.parallelism
spark.conf.set("spark.default.parallelism", "400")  // 通常是 cores × 2~3

// 第二种：显式指定分区数
rdd.reduceByKey(_ + _, 400)
df.groupBy("key").agg(sum("val")).repartition(400)
```

## 面试高频考点

### Q: Spark Shuffle 和 Hadoop MapReduce Shuffle 的区别？

| 维度 | Spark SortShuffle | Hadoop MR Shuffle |
|------|------------------|-------------------|
| 写阶段 | Map 端排序 + 合并为一个文件 | Map 端分区 + 排序 + 多文件 |
| 读阶段 | Netty 框架拉取，支持本地短路读 | HTTP 拉取 |
| 中间文件 | 一个 index + 一个 data 文件 | 按分区多个文件（MapR 可合并） |
| 排序 | 可按需跳过排序 | 必须排序 |

### Q: 为什么要对 Shuffle 数据进行排序？

1. 方便 Reduce 端归并（Sort Merge Join）
2. 支持 range 分区
3. 减少内存碎片（ExternalSorter 依赖排序溢写）

### Q: `spark.sql.shuffle.partitions` 默认 200 是不是太小了？

对于大集群（数百 Executor），200 确实太小——每个 Task 处理的数据量太大。建议设置为 `cores × 2~3`（例如 1000 核 → 2000-3000 分区）。

### Q: Shuffle 失败 (FetchFailedException) 怎么处理？

1. 检查 Executor 是否 OOM（增大 spark.executor.memory）
2. 增大 `spark.shuffle.io.maxRetries`（默认 3）
3. 检查网络稳定性
4. 如果 Stage 重试了 4 次仍失败，整个 Job 会失败

## 小结

| 层面 | 核心优化策略 |
|------|-------------|
| Map 端 | 增大 spark.shuffle.file.buffer、用 Bypass 机制 |
| Reduce 端 | 控制 `maxSizeInFlight`、增加 fetch 线程数 |
| 内存 | shuffle.memoryFraction 调优 |
| 数据倾斜 | 两阶段聚合、Map Join、加盐 Join、调并行度 |
| 文件数 | SortShuffle 每个 Map Task 就 2 个文件 |
