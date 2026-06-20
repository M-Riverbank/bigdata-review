# Spark Core — RDD 核心原理

## RDD 是什么

RDD（Resilient Distributed Dataset）是 Spark 最核心的抽象，表示一个**不可变、分区的、可并行操作**的数据集合。

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

### RDD 是不可变的

```scala
val rdd = sc.parallelize(1 to 100)
// rdd.map(...)  创建新的 RDD，原 RDD 不变
// RDD 上的所有转换（transformation）都返回新 RDD
```

> **为什么不可变**：分布式场景下，修改数据的一致性无法在实时保证（需要分布式事务）。不可变 + 重新计算是更简单正确的模型。

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

## RDD 血缘（Lineage）

血缘是 RDD 的**容错基石** — 记录了每个 RDD 如何从父 RDD 和原始数据计算而来。

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

### Stage 划分（面试必考！）

Spark 遇到**宽依赖**就切割 Stage：

- **窄依赖**：父 RDD 的每个分区最多被一个子 RDD 分区使用（不需要 Shuffle）
  - `map`、`filter`、`flatMap`、`mapPartitions`
  - 父分区 → 子分区 1:1 或 N:1

- **宽依赖**：父 RDD 的一个分区被多个子 RDD 分区使用（需要 Shuffle）
  - `reduceByKey`、`groupByKey`、`groupBy`、`join`（非 broadcast）
  - 父分区 → 子分区 1:N

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

## 算子分类

### Transformation（转换，惰性）

| 算子 | 类型 | 说明 |
|------|------|------|
| `map(f)` | 窄 | 一对一映射 |
| `flatMap(f)` | 窄 | 一对多展开 |
| `filter(f)` | 窄 | 过滤 |
| `mapPartitions(f)` | 窄 | 按分区批量操作 |
| `distinct()` | 宽 | 去重（需要 Shuffle） |
| `reduceByKey(f)` | 宽 | 按键聚合（有 Map 端预聚合） |
| `groupByKey()` | 宽 | 按键分组（无预聚合） |
| `sortByKey()` | 宽 | 按键排序 |
| `join(other)` | 宽 | 内连接（可优化为窄） |
| `repartition(n)` | 宽 | 重分区（全量 Shuffle） |
| `coalesce(n)` | 窄 | 减少分区（默认不 Shuffle） |

### Action（动作，触发计算）

| 算子 | 说明 |
|------|------|
| `collect()` | 拉取到 Driver（慎用！） |
| `count()` | 行数 |
| `take(n)` | 取前 n 条 |
| `first()` | 第一条 |
| `reduce(f)` | 归约 |
| `foreach(f)` | 遍历 |
| `saveAsTextFile(path)` | 保存 |
| `countByKey()` | 按键统计 |

## 共享变量

### Broadcast（广播变量）

```scala
val smallTable = Map(1 -> "张三", 2 -> "李四", 3 -> "王五")
val broadcastTable = sc.broadcast(smallTable)

largeRDD.map { record =>
  val lookup = broadcastTable.value  // 从本地内存读取
  (record, lookup.getOrElse(record.userId, "未知"))
}
```

**原理**：数据只发送到每个 Executor 一次（不是每个 Task 一次），存在 Executor 内存中。

**使用条件**：
- 数据能放入 Executor 内存（通常 < 几百 MB）
- 在多个 Task 中只读使用
- 比 Shuffle Join 的效率高一个量级

### Accumulator（累加器）

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

## 持久化与缓存

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

| 缓存级别 | 位置 | 序列化 | 说明 |
|---------|------|--------|------|
| MEMORY_ONLY | 内存 | 否 | 默认，最快但最占内存 |
| MEMORY_AND_DISK | 内存+磁盘 | 否 | 内存不够溢写磁盘 |
| MEMORY_ONLY_SER | 内存 | 是 | 省内存、多 CPU 开销 |
| DISK_ONLY | 磁盘 | 是 | 最慢但最安全 |

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
