# Spark Core — RDD 算子详解

## RDD 算子分类总览

在 Spark 中，RDD 算子分为两大类：**Transformation**（转换）和 **Action**（动作）。这不仅仅是概念上的区分，更深刻影响着 Spark 的**调度模型**和**性能表现**。

> **面试点**：Spark 之所以比 Hadoop MapReduce 快，一个核心原因就是 Transformation 的惰性求值（Lazy Evaluation）机制——它让 Spark 有机会构建 DAG 执行计划，而不是每一步都落盘。

### Transformation：惰性求值，构建 DAG

调用 Transformation 时，Spark **不会立即执行**，而是构建一个 **RDD 的血缘关系 Lineage**（DAG 有向无环图）。每一个 Transformation 操作都会生成一个新的 RDD 节点，记录其父 RDD 和计算逻辑。只有当 Action 触发时，Spark 才会回溯整个 DAG 并生成物理执行计划。

### Action：触发计算，产出结果

Action 是 DAG 的"开关"——遇到 Action 时，Spark 的 DAGScheduler 才会将 DAG 切分为 Stage，提交到集群执行。

> **踩坑经验**：刚学 Spark 的时候很容易犯一个错——写了一大串 Transformation，觉得已经"算完"了，结果 `collect()` 一下全没了，才反应过来数据压根没算过。新手记住：**没有 Action 的 Transformation 就是白干**。

### 完整分类图谱

| 大类 | 子类 | 典型算子 | 是否 Shuffle |
|------|------|---------|:----------:|
| Transformation | 单元素 | `map` / `filter` / `flatMap` / `mapPartitions` | 不 |
| Transformation | Key-Value 聚合 | `reduceByKey` / `aggregateByKey` / `combineByKey` | 是 |
| Transformation | Key-Value 分组 | `groupByKey` / `cogroup` | 是 |
| Transformation | Join | `join` / `leftOuterJoin` / `fullOuterJoin` | 是 |
| Transformation | 分区 | `coalesce` / `repartition` / `partitionBy` | 视情况 |
| Transformation | 集合 | `union` / `intersection` / `subtract` / `cartesian` | 视情况 |
| Action | 聚合 | `reduce` / `fold` / `aggregate` | 不 |
| Action | 收集 | `collect` / `take` / `first` / `top` | 不 |
| Action | 计数 | `count` / `countByKey` / `countByValue` | 不 |
| Action | 保存 | `saveAsTextFile` / `saveAsSequenceFile` | 不 |

```
Transformation（惰性，返回新 RDD）
├── 单元素类型
│   ├── map / flatMap / filter / mapPartitions
│   └── distinct / coalesce / repartition
├── Key-Value 类型
│   ├── reduceByKey / groupByKey / aggregateByKey
│   ├── sortByKey / mapValues / keys / values
│   └── join / cogroup / leftOuterJoin / fullOuterJoin
└── 数学/集合类
    ├── union / intersection / subtract / cartesian
    └── zip / zipWithIndex

Action（触发计算，返回结果）
├── 聚合类: reduce / fold / aggregate
├── 收集类: collect / take / first / top / takeSample
├── 计数类: count / countByKey / countByValue
├── 保存类: saveAsTextFile / saveAsSequenceFile
└── 遍历类: foreach / foreachPartition
```

## 单元素类型 Transformation

为什么需要这些算子？因为大数据处理的第一要务就是**对海量数据做逐条或逐分区的变换**——过滤掉脏数据、格式转换、字段提取，所有 ETL 都离不开它们。

### map 与 mapPartitions

**原则**：`map` 是"一对一"（一条进一条出），`mapPartitions` 是"一个分区出一批"（进一个 Iterator，出一个 Iterator）。

```scala
// map — 逐元素变换（一对一）
val rdd = sc.parallelize(1 to 10)
rdd.map(_ * 2).collect()
// res: Array(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)

// mapPartitions — 按分区批量操作（减少函数调用次数）
rdd.mapPartitions { iter =>
  // 每个分区调用一次，iter 是分区内所有元素的迭代器
  val maxVal = iter.max
  Iterator(maxVal)
}
```

> **区别**：`map` 每条记录调用一次函数；`mapPartitions` 每个分区调用一次。后者适合需要批量初始化资源的场景（如数据库连接），但每个分区可能会 OOM。

| 对比维度 | `map` | `mapPartitions` |
|---------|------|----------------|
| 调用次数 | 每条记录一次 | 每个分区一次 |
| 资源开销 | 小，用完即释放 | 大，分区内持有整个 Iterator |
| 适用场景 | 简单字段转换 | 批量初始化（DB 连接、网络请求） |
| OOM 风险 | 无 | 有——分区内数据量大时 |
| 函数调用开销 | 高（N 条记录 = N 次函数调用） | 低（N 条记录 = P 个分区次调用） |

**什么时候用 `mapPartitions`？** 典型场景是每条记录需要建立外部资源连接（比如 MySQL 连接池）。如果每条记录都 `map` 建立连接，N 条记录就建立 N 次连接，代价太大。用 `mapPartitions`，每个分区只建立一次连接，性能飞跃。

```scala
// 实战：批量写入 MySQL
rdd.mapPartitions { iter =>
  val conn = DriverManager.getConnection("jdbc:mysql://...")
  val stmt = conn.prepareStatement("INSERT INTO table VALUES (?)")
  iter.foreach { record =>
    stmt.setString(1, record)
    stmt.addBatch()
  }
  stmt.executeBatch()
  conn.close()
  Iterator.empty
}
```

> **踩坑经验**：`mapPartitions` 里一定要记得关闭资源（在 `finally` 块里）。如果你在 WebUI 上看到大量连接未释放，十有八九是 `mapPartitions` 里的资源没关干净。另外不要用 `iter.toList` 直接把整个分区装进内存——那等于放弃了 Spark 的流式处理优势，数据量大的分区会直接 OOM。

### flatMap

为什么需要 `flatMap`？因为现实世界中的数据往往是一条记录里塞了多份信息——比如一句话里有多个单词、一个 JSON 数组字段里有多个元素。`flatMap` 就是帮你做"展平"的。

```scala
// flatMap — 一对多展开
val rdd = sc.parallelize(Seq("hello world", "hello spark"))
rdd.flatMap(_.split(" ")).collect()
// res: Array(hello, world, hello, spark)
```

> **面试点**：`flatMap` 本质上等于 `map` + `flatten`。如果你在 `flatMap` 的匿名函数里返回了一个普通值而不是集合，编译时就会报类型错误——因为 `flatMap` 要求返回 `TraversableOnce`。

```scala
// flatMap 的典型应用：提取日志中的 IP 地址
val logs = sc.parallelize(Seq(
  "192.168.1.1 - - [01/Jan/2023:12:00:00] GET /index.html",
  "10.0.0.1 - - [01/Jan/2023:12:01:00] POST /api/data"
))
logs.flatMap("""(\d+\.\d+\.\d+\.\d+)""".r.findFirstIn(_)).collect()
// res: Array(192.168.1.1, 10.0.0.1)
```

### filter

为什么需要 `filter`？数据清洗永远是第一步——空值、异常值、无效记录，统统过滤掉。

```scala
val rdd = sc.parallelize(1 to 100)
rdd.filter(_ % 2 == 0).count()
// res: 50
```

**实战：数据清洗**

```scala
// 过滤掉空行和注释行
val cleanRDD = rawRDD
  .filter(line => line != null && line.trim.nonEmpty)
  .filter(line => !line.startsWith("#"))

// 过滤掉异常值（如年龄超出合理范围）
val validRDD = dataRDD.filter { record =>
  val age = record.age
  age > 0 && age < 150
}
```

> **踩坑经验**：`filter` 之后 RDD 的**分区数不变**，即使某些分区被过滤成了空分区。这可能导致后续 `mapPartitions` 出现空 Iterator。如果要减少分区，在 filter 后配合 `coalesce` 使用。

### 分区操作

为什么需要分区操作？因为 Spark 的性能瓶颈往往不是 CPU，而是**数据分布不均**。分区数太少无法利用并行度，分区数太多带来调度开销，数据倾斜更是 Shuffle 的大敌。

```scala
// coalesce — 减少分区数（不 Shuffle，合并相邻分区）
rdd.coalesce(2)  // 4个分区 → 2个分区（安全减少）

// repartition — 增加或减少分区（全量 Shuffle）
rdd.repartition(8)  // 4个分区 → 8个分区

// mapPartitionsWithIndex — 带分区索引的 mapPartitions
rdd.mapPartitionsWithIndex { (idx, iter) =>
  Iterator(s"Partition[$idx] has ${iter.size} elements")
}
```

| 算子 | Shuffle？ | 分区数变化 | 开销 | 典型场景 |
|------|:---------:|:---------:|:----:|---------|
| `coalesce` | 否（默认） | 只减不增 | 低 | 减少分区以适配下游 |
| `coalesce(true)` | 是 | 只减不增 | 中 | 需要均匀分布时 |
| `repartition` | 是 | 增减均可 | 高 | 增加并行度 |
| `partitionBy` | 是 | 指定分区数 | 高 | 数据倾斜优化 |

> **面试点**：`coalesce` 默认 `shuffle = false`，通过合并相邻分区减少分区数。为什么不能增加分区？因为不 Shuffle 就无法把数据打散到更多分区。
> 如果你发现 `coalesce` 后出现了数据倾斜（某些分区数据特别多），可以用 `coalesce(5, shuffle = true)` 强制进行一轮 Shuffle 来均匀分布。

**数据倾斜排查技巧**：通过 `mapPartitionsWithIndex` 查看各分区数据量，快速定位倾斜分区：

```scala
rdd.mapPartitionsWithIndex { (idx, iter) =>
  Iterator(s"Partition $idx: ${iter.size} records")
}.collect().foreach(println)
```

## Key-Value 类型 Transformation

为什么需要 Key-Value 算子？因为大数据处理的核心就是"分组聚合"——按 key 归并、计算、Join。Hadoop MapReduce 就是从 Map 端的 Key 分发开始设计的。Spark 的 Key-Value 算子本质上就是在做同样的事，但做得更高效。

### reduceByKey vs groupByKey

这两个算子是大数据面试中 **出现频率最高的 Spark 题**，没有之一。

```scala
val pairs = sc.parallelize(Seq(("a", 1), ("b", 1), ("a", 2), ("b", 3)))

// reduceByKey — 先 Map 端预聚合，再 Shuffle
pairs.reduceByKey(_ + _).collect()
// res: Array(("a", 3), ("b", 4))

// groupByKey — 不预聚合，全部 Shuffle（慎用！）
pairs.groupByKey().mapValues(_.sum).collect()
// res: Array(("a", 3), ("b", 4))
```

> **面试必考**：`reduceByKey` 在 Map 端做 Combiner（预聚合），Shuffle 数据量小。`groupByKey` 把所有原始 KV 都 Shuffle，性能差很多。

| 对比维度 | `reduceByKey` | `groupByKey` |
|---------|:-------------:|:------------:|
| Map 端预聚合 | 是（Combiner） | 否 |
| Shuffle 数据量 | 小（已聚合） | 大（全量原始数据） |
| 内存压力 | 低 | 高（单个 key 数据量大时 OOM） |
| 适用场景 | 可结合/交换的聚合操作 | 必须保留全部值的场景 |
| 使用建议 | **优先使用** | 尽量避免 |

**底层原理**：`reduceByKey` 之所以能做 Map 端预聚合，是因为它的聚合函数满足**结合律和交换律**（`_ + _`）。Spark 在 Map 端对每个分区先做一次本地 `reduce`，只把 reduce 后的结果 Shuffle 出去。而 `groupByKey` 不知道你要做什么操作，只能把全部数据原封不动传过去。

> **踩坑经验**：有一个常见误区——认为 `groupByKey` 一定比 `reduceByKey` 慢。其实如果每个 key 对应的 value 很少（比如 1-2 个），两者的 Shuffle 数据量差别不大。真正要命的是**单个 key 对应海量 value**的场景（比如数据倾斜），此时 `groupByKey` 会把所有 value 塞进一个 Iterable 放进内存，直接 OOM。

### aggregateByKey

为什么需要 `aggregateByKey`？因为 `reduceByKey` 和 `groupByKey` 各有局限——前者要求分区内和分区间的计算逻辑相同，后者不能自定义初始值。`aggregateByKey` 把灵活度拉满。

```scala
// aggregateByKey — 更灵活的聚合（分区内 + 分区间不同逻辑）
// 语法：aggregateByKey(zeroValue)(seqOp, combOp)

val data = sc.parallelize(Seq(
  ("A", 80), ("A", 90), ("B", 85), ("B", 95)
))

// 计算每个 key 的总分和个数 → 平均分
data.aggregateByKey((0, 0))(
  (acc, score) => (acc._1 + score, acc._2 + 1),  // 分区内：累加
  (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // 分区间：合并
).mapValues { case (sum, cnt) => sum.toDouble / cnt }.collect()
// Array(("A", 85.0), ("B", 90.0))
```

> **面试点**：`aggregateByKey` 的核心概念是 **zeroValue**——它是每个分区聚合的起点。注意 zeroValue 会在每个分区被使用一次，且分区间合并时也会被用到，所以 zeroValue 必须满足"中性"条件：`combOp(zeroValue, x) = x`。

**实战：统计每个用户的浏览记录（同时统计最大值和最小值）**

```scala
val userData = sc.parallelize(Seq(
  ("user1", 100), ("user1", 200), ("user1", 50),
  ("user2", 300), ("user2", 150)
))

userData.aggregateByKey((Int.MaxValue, Int.MinValue, 0))(
  (acc, v) => (math.min(acc._1, v), math.max(acc._2, v), acc._3 + 1),
  (a1, a2) => (math.min(a1._1, a2._1), math.max(a1._2, a2._2), a1._3 + a2._3)
).collect()
// 结果：每个用户的（最小值，最大值，记录数）
```

### combineByKey

`combineByKey` 是 Key-Value 聚合算子中**最通用**的一个，是 `reduceByKey`、`aggregateByKey` 的底层实现。面试中偶尔会考察其三个函数参数的含义：

```scala
// combineByKey(createCombiner, mergeValue, mergeCombiners, partitioner)
val nums = sc.parallelize(Seq(("a", 1), ("b", 2), ("a", 3), ("b", 4)))

nums.combineByKey(
  (v: Int) => (v, 1),            // createCombiner：遇到新 key，初始化 (value, count)
  (acc: (Int, Int), v: Int) => (acc._1 + v, acc._2 + 1),  // mergeValue：分区内合并
  (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)  // mergeCombiners：分区间合并
).mapValues { case (sum, cnt) => sum.toDouble / cnt }.collect()
// Array(("a", 2.0), ("b", 3.0)) — 每个 key 的平均值
```

> **面试点**：`combineByKey` 的三个函数参数体现了 MapReduce 的核心思想——Map 端初始化、Map 端合并、Reduce 端合并。理解了这个，你就理解了分布式聚合的全部。

### sortByKey

排序是数据处理中非常高频的需求——Top N、排行榜、时间序列分析都离不开它。

```scala
val rdd = sc.parallelize(Seq(("b", 2), ("a", 1), ("c", 3)))

// 默认升序
rdd.sortByKey().collect()
// Array(("a", 1), ("b", 2), ("c", 3))

// 降序
rdd.sortByKey(ascending = false).collect()
// Array(("c", 3), ("b", 2), ("a", 1))
```

> **踩坑经验**：`sortByKey` 会触发一次全量 Shuffle（因为需要全局排序），如果数据量极大，建议先 `coalesce` 减少分区数量以避免小文件过多。它的底层使用 RangePartitioner 做采样分区，所以比 `collect().sort()` 高效得多。

### Join 系列

为什么需要 Join？因为现实数据永远是分散存储的——用户信息一张表、订单数据一张表，分析时必须关联起来。Spark 的 Join 系列算子就是做这件事的。

```scala
val rdd1 = sc.parallelize(Seq(("a", 1), ("b", 2), ("c", 3)))
val rdd2 = sc.parallelize(Seq(("a", 10), ("b", 20), ("d", 30)))

rdd1.join(rdd2).collect()
// Array(("a", (1,10)), ("b", (2,20)))  — 内连接，只保留两边都有的 key

rdd1.leftOuterJoin(rdd2).collect()
// Array(("a", (1,Some(10))), ("b", (2,Some(20))), ("c", (3,None)))

rdd1.fullOuterJoin(rdd2).collect()
// Array(("a", (Some(1),Some(10))), ("d", (None,Some(30))), ...)
```

| Join 类型 | 保留哪些 key | 缺失值处理 | 返回类型 |
|----------|:------------:|:----------:|:--------:|
| `join` | 两边都有的 key | 无缺失 | `(K, (V, W))` |
| `leftOuterJoin` | 左 RDD 全部 key | 右缺失为 None | `(K, (V, Option[W]))` |
| `rightOuterJoin` | 右 RDD 全部 key | 左缺失为 None | `(K, (Option[V], W))` |
| `fullOuterJoin` | 两边全部 key | 缺失为 None | `(K, (Option[V], Option[W]))` |

> **面试点——Join 的 Shuffle 机制**：两个 RDD 做 Join 时，如果它们有相同的 Partitioner（比如都 HashPartitioned 到相同的分区数），那么 Join 是**窄依赖**，不需要 Shuffle。如果没有共同分区器，两边都要 Shuffle（宽依赖）。所以**先用 `partitionBy` 再多次 Join** 是经典优化手段。

### cogroup

为什么需要 `cogroup`？因为有时候你并不想 Join（把不同 RDD 的值配成一对），而只是想**按 key 分组**保留所有原始数据。`cogroup` 的返回值保留了每个 RDD 中的完整集合。

```scala
// cogroup — 多个 RDD 按 key 分组（不 Join，只分组）
rdd1.cogroup(rdd2).collect()
// Array(
//   ("a", (CompactBuffer(1), CompactBuffer(10))),
//   ("b", (CompactBuffer(2), CompactBuffer(20))),
//   ("c", (CompactBuffer(3), CompactBuffer())),
//   ("d", (CompactBuffer(), CompactBuffer(30)))
// )
// join 底层基于 cogroup 实现
```

> **踩坑经验**：`cogroup` 返回的是 `(K, (Iterable[V], Iterable[W]))`，如果单个 key 下某个 RDD 的数据量特别大，整个 `Iterable` 会在计算时加载到内存。这时候同样面临 OOM 风险。

## Action 详解

为什么需要了解各种 Action？因为新手最容易犯的错误就是**只用一个 `collect()` 解决所有问题**——结果数据太大把 Driver 撑爆了还不知道怎么回事。

### collect / take / first

```scala
// collect — 拉取全部到 Driver（数据量大时慎用！会 OOM）
rdd.collect().foreach(println)

// take — 取前 N 条（比 filter + collect 更高效）
rdd.take(10)

// first — 取第一条
rdd.first()
```

| Action | 返回 | 数据量限制 | Driver 压力 | 推荐场景 |
|--------|:----:|:---------:|:-----------:|---------|
| `collect` | `Array[T]` | 无 | 极大——全部数据到 Driver | 调试 + 数据量小 |
| `take(N)` | `Array[T]` | N 条 | 极小 | 数据采样、预览 |
| `first` | `T` | 1 条 | 极小 | 快速验证数据格式 |
| `top(N)` | `Array[T]` | N 条排序后 | 中等 | Top N 排行榜 |
| `takeSample` | `Array[T]` | N 条随机 | 极小 | 随机采样 |

> **踩坑经验**：`collect()` 是新手最容易掉进去的坑。生产环境中一个 RDD 可能有几亿条记录，你在 WebUI 日志里看到 Driver OOM 的第一个排查点就是代码里有没有 `collect()`。学会用 `take(10)` 来预览数据，用 `count()` 来查看数据量，这才是生产级别的习惯。

### reduce / fold / aggregate

这三个算子对应 Transformation 中 `reduceByKey` / `foldByKey` / `aggregateByKey` 的 Action 版本——区别在于它们不按 key 分组，而是对整个 RDD 做规约。

```scala
val nums = sc.parallelize(1 to 100)

// reduce — 两两归并
nums.reduce(_ + _)  // 5050

// aggregate — 零值 + 分区内 + 分区间（最通用）
nums.aggregate(0)(_ + _, _ + _)  // 5050，类似 reduce

// fold — reduce 的 zeroValue 版本
nums.fold(0)(_ + _)
```

| 算子 | 是否需要 zeroValue | 分区内与分区间逻辑 | 灵活度 |
|------|:-----------------:|:-----------------:|:------:|
| `reduce` | 否（用第一个元素） | 相同 | 低 |
| `fold` | 是 | 相同 | 中 |
| `aggregate` | 是 | 可以不同 | 高 |

**实战：用 aggregate 同时求最大值和最小值**

```scala
nums.aggregate((Int.MaxValue, Int.MinValue))(
  (acc, v) => (math.min(acc._1, v), math.max(acc._2, v)),  // 分区内
  (a1, a2) => (math.min(a1._1, a2._1), math.max(a1._2, a2._2))  // 分区间
)
// res: (1, 100)
```

注意这里 `zeroValue = (Int.MaxValue, Int.MinValue)` 的巧妙之处——最小值初始要设最大数（`Int.MaxValue`），最大值初始要设最小数（`Int.MinValue`），保证第一个输入就能正确替换。

> **面试点**：`reduce` 和 `fold` 要求运算满足**结合律**（Associative），因为分布式环境下数据会分区分批聚合。如果你传一个不满足结合律的函数（比如 `_ - _`），不同分区顺序会导致结果不一致。

### foreach 与 foreachPartition

```scala
// foreach — 逐条处理（每条一个函数调用）
rdd.foreach(println)  // 注意：输出到各 Executor 的 stdout，不是 Driver 的！

// foreachPartition — 按分区批量处理
rdd.foreachPartition { iter =>
  val conn = createConnection()  // 每个分区建一次连接
  iter.foreach { record =>
    conn.send(record)
  }
  conn.close()
}
```

> **踩坑经验**：很多新手在 RDD 里写 `rdd.foreach(println)`，以为能在 Driver 端看到输出，结果啥都没看到。这是因为 `foreach` 在各个 Executor 上执行，输出到 Executor 的 stdout——只有在 YARN `yarn logs -applicationId` 里才能看到。想调试的话，用 `rdd.take(10).foreach(println)` 或 `rdd.collect().foreach(println)` 在 Driver 侧输出。

### saveAsTextFile

```scala
rdd.saveAsTextFile("hdfs:///output/result")
// 不保存为单个文件，而是保存为目录，内部分区文件：
// _SUCCESS — 空文件，标记成功
// part-00000, part-00001, ... — 各分区的输出
```

**为什么保存出来的是一个目录而不是一个文件？** 因为每个 Executor 的每个分区独立写出，并行度高，但可能会产生大量小文件。如果你需要合并为单个文件：

```scala
// 方法1：先 coalesce(1) 再 save（但是丧失了并行度）
rdd.coalesce(1).saveAsTextFile("hdfs:///output/merged")

// 方法2：用 Hadoop API 做后续合并（推荐）
// hadoop fs -getmerge /output/result /local/result.txt
```

> **踩坑经验**：`coalesce(1)` + `saveAsTextFile` 虽然能得到一个文件，但所有数据在一个分区处理，大数据量时极其缓慢，且容易 OOM。如果有大量数据需要输出为单个文件，宁可后续用 `hadoop fs -getmerge` 或 Spark 的 `FileUtil.copyMerge` 来合并。

### countByKey 与 countByValue

```scala
val rdd = sc.parallelize(Seq(("a", 1), ("b", 1), ("a", 2)))

rdd.countByKey()     // Map("a" -> 2, "b" -> 1)
rdd.countByValue()   // Map(("a", 1) -> 1, ("b", 1) -> 1, ("a", 2) -> 1)
```

> **注意**：这两个算子返回的是 Map，会放到 Driver 内存中。如果 key 的种类特别多（比如上亿个不同 key），Driver 可能 OOM。这种情况下，用 `reduceByKey` + `collect` 更安全。

## 分区器（Partitioner）

为什么需要分区器？因为 Spark 的 Shuffle 依赖于"把相同 key 的数据送到同一个分区"。分区器的选择直接影响 Shuffle 的数据量、是否发生 Shuffle，以及数据是否均匀分布。

### HashPartitioner 与 RangePartitioner

```scala
// HashPartitioner — 默认（key.hashCode % numPartitions）
val partitioned = pairs.partitionBy(new HashPartitioner(4))

// RangePartitioner — 按 key 范围分区（适合排序）
import org.apache.spark.RangePartitioner
val rangePartitioned = pairs.partitionBy(
  new RangePartitioner(4, pairs)
)
```

| 分区器 | 分区依据 | 特点 | 适用场景 |
|--------|:-------:|:----:|---------|
| `HashPartitioner` | `key.hashCode % N` | 速度快，但可能导致不均匀 | 默认选择，通用场景 |
| `RangePartitioner` | key 的范围区间 | 需要采样，分区均匀 | 排序 + 全局有序 |

### 分区器影响

分区器的选择不仅是数学问题，更是**性能优化**的核心手段：

- **同分区器**：两个 RDD 相同分区器时，`join` 不再 Shuffle（窄依赖）
- **无分区器**：默认 HashPartitioner，分区数 = `spark.default.parallelism`

```scala
// 优化技巧：先 partitionBy 再多次 join
val partitioned = largeRDD.partitionBy(new HashPartitioner(100)).cache()
partitioned.join(otherRDD)  // 不会 Shuffle！
partitioned.join(anotherRDD) // 也不会 Shuffle！
```

**这个优化为什么有效？** 第一次 `partitionBy` 触发一次 Shuffle，把数据按 Hash 分到 100 个分区。后续多次 `join` 时，只要 `otherRDD` 和 `anotherRDD` 也使用相同的分区器（或者被 Spark 自动认定为相同），那么 Join 时两个 RDD 的数据已经在同一个分区里了，不需要再 Shuffle！

> **面试点**：这一段几乎是 Spark 优化的必考题。记住公式：**`partitionBy` + `cache` + 多次 `join` = 一次 Shuffle 换 N 次 Shuffle 免除**。数据量越大，收益越明显。

### 自定义分区器

当内置分区器无法满足需求时，可以自定义：

```scala
class DomainPartitioner(numParts: Int) extends Partitioner {
  override def numPartitions: Int = numParts

  override def getPartition(key: Any): Int = {
    val domain = key.toString.split("@")(1)  // 按邮箱域名分区
    domain.hashCode % numPartitions
  }
}

val emails = sc.parallelize(Seq(
  ("user1@gmail.com", "data1"),
  ("user2@qq.com", "data2"),
  ("user3@gmail.com", "data3")
))
emails.partitionBy(new DomainPartitioner(4))
```

## 小结

| 类别 | 关键算子 | 面试频率 |
|------|---------|---------|
| 基础转换 | map / flatMap / filter | ⭐⭐⭐ |
| 聚合 | reduceByKey / aggregateByKey | ⭐⭐⭐⭐⭐ |
| Join | join / cogroup / leftOuterJoin | ⭐⭐⭐⭐ |
| 分区 | partitionBy / coalesce / repartition | ⭐⭐⭐ |
| Action | collect / count / saveAsTextFile | ⭐⭐ |

### 面试高频问题速查

| 问题 | 核心回答 |
|------|---------|
| `map` 和 `mapPartitions` 的区别 | 调用次数 vs 批量处理，资源初始化场景 |
| `reduceByKey` 为什么比 `groupByKey` 快 | Map 端 Combiner 预聚合 |
| `coalesce` 和 `repartition` 的区别 | 是否触发 Shuffle |
| 什么情况下 Join 不会 Shuffle | 两个 RDD 有相同 Partitioner |
| `collect` 为什么危险 | 全量数据拉到 Driver，大数据量会 OOM |

### 学习建议

1. **优先掌握**：`map` / `flatMap` / `filter` / `reduceByKey` / `join` / `collect` —— 覆盖 80% 日常开发场景
2. **性能敏感**：理解哪些算子触发 Shuffle，Shuffle 是 Spark 最昂贵的操作
3. **生产习惯**：用 `take(N)` 替代 `collect()` 预览数据，用 `count()` 了解数据规模
4. **面试重点**：`reduceByKey vs groupByKey`、`Shuffle 优化`、`数据倾斜` 三大高频话题
