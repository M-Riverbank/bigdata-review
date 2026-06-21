# Spark SQL DataFrame API 深入

## 为什么需要 DataFrame 和 Dataset

Spark 1.x 时代，开发者面对的核心抽象是 RDD（弹性分布式数据集）。RDD 虽然灵活，但有两个致命缺陷：

1. **无 Schema 信息**：RDD 不知道每列的数据类型，无法做针对性优化
2. **序列化开销大**：Java/Kryo 序列化大量小对象，GC 压力巨大

想象一下：你用 RDD 处理一个 100 列的 Parquet 文件，即使你只需要其中 3 列，RDD 也必须把全部 100 列读出来。因为你没有告诉 Spark 你的数据长什么样。

DataFrame 和 Dataset 的出现彻底解决了这个问题：

| 问题 | RDD 的方式 | DataFrame/Dataset 的方式 |
|------|-----------|------------------------|
| 列裁剪 | 做不到，必须读取全部列 | 自动只读取需要的列 |
| 过滤优化 | 必须读全量数据再过滤 | 谓词下推到数据源层面 |
| 序列化 | Java/Kryo 全对象序列化 | Tungsten 堆外二进制存储 |
| 类型检查 | 运行时才能发现类型错误 | 编译期（Dataset）或运行时（DataFrame） |

## DataFrame vs Dataset vs RDD

```scala
// RDD — 无 Schema，编译期无类型检查
val rdd: RDD[Person] = ...
rdd.filter(_.age > 18)  // age 是什么？仅运行时知道

// DataFrame — 有 Schema，运行时类型
val df: DataFrame = spark.read.parquet("...")
df.filter($"age" > 18)  // $"age" 是 Column 表达式
df.filter("age > 18")   // SQL 表达式

// Dataset — 有 Schema + 编译期类型检查
val ds: Dataset[Person] = df.as[Person]
ds.filter(_.age > 18)  // 编译期类型安全！
```

| 维度 | RDD | DataFrame | Dataset[T] |
|------|-----|-----------|------------|
| Schema | 无 | 有 | 有 |
| 类型安全 | 编译期（泛型） | 运行时 | 编译期 |
| 优化器 | 无（用户手动调优） | Catalyst（自动） | Catalyst（自动） |
| 序列化 | Java/Kryo | Tungsten Row | Encoder |
| API | 函数式 | 声明式 SQL | 函数式 + SQL |
| GC 影响 | 大（对象多） | 小（堆外） | 小 |
| Python 支持 | 是 | 是 | 否 |
| 数据源类型 | 不感知 | Schema 感知 | Schema 感知 |

> **面试点**：面试中常被问到"为什么 Dataset 的序列化比 RDD 高效？"核心原因是 Encoder 机制。Encoder 直接操作 Tungsten 的堆外二进制格式，不需要创建大量 Java 对象，避免了 GC 开销。而 RDD 的 Java/Kryo 序列化需要将每个对象反序列化成 JVM 对象才能操作。

### RDD to DataFrame 的演进逻辑

```
RDD[T]                     → 无 Schema，手工优化
     ↓ (toDF)
DataFrame = Dataset[Row]   → 有 Schema，Catalyst 自动优化
     ↓ (.as[T])
Dataset[T]                 → 类型安全，Encoder 序列化
```

## DataFrame 基础操作

### 创建 DataFrame

```scala
// 1. 从文件
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("path/to/file.csv")

// 2. 从 RDD
import spark.implicits._
val df = rdd.toDF("col1", "col2")

// 3. 从集合
val df = Seq(
  ("张三", 25, "北京"),
  ("李四", 30, "上海")
).toDF("name", "age", "city")

// 4. 手动构建 Schema
import org.apache.spark.sql.types._
val schema = StructType(Array(
  StructField("name", StringType),
  StructField("age", IntegerType),
  StructField("city", StringType)
))
val df = spark.createDataFrame(rowRDD, schema)
```

> **踩坑经验**：从 CSV 读取时用 `inferSchema = true` 会让 Spark 扫描前 1000 行来推断类型，如果数据量大或者列数多，这个扫描本身就是一次作业，会造成额外开销。推荐的做法是手动指定 Schema，既快又准确。

### 查看 DataFrame

```scala
// 打印 Schema 结构
df.printSchema()

// 预览前 N 行
df.show(5, truncate = false)  // truncate = false 不截断长字符串

// 统计信息（在探索数据时很有用）
df.describe().show()

// 行数
df.count()

// 去重统计
df.select("city").distinct().count()
```

### Select 操作

```scala
// 选择列
df.select("name", "age")
df.select($"name", $"age" + 1)  // 表达式
df.selectExpr("name", "age + 1 as age_next_year")  // SQL 表达式

// 条件选择
df.filter($"age" > 18)
df.where("age > 18")  // filter 别名
df.filter($"city".isin("北京", "上海"))
df.filter($"name".startsWith("张"))
```

> **面试点**：`select`、`selectExpr`、`withColumn` 有什么区别？`select` 只能选择已有列；`selectExpr` 支持 SQL 表达式；`withColumn` 新增或替换单列。执行效率上三者在 Catalyst 优化后差别不大，选择取决于代码可读性。

### 聚合操作

```scala
// 简单聚合
df.groupBy("city").count()

// 多聚合
df.groupBy("city").agg(
  count("name").as("cnt"),
  avg("age").as("avg_age"),
  max("age").as("max_age"),
  min("age").as("min_age")
)

// 对全表聚合（无 groupBy）
df.agg(sum("amount"), avg("amount"))

// 高级聚合
import org.apache.spark.sql.functions._

df.agg(
  countDistinct("user_id"),              // 精确去重计数
  approx_count_distinct("user_id", 0.01), // 近似去重（HyperLogLog）
  collect_set("category"),               // 收集去重列表
  collect_list("category"),              // 收集列表（可能重复）
  skewness("amount"),                    // 偏度
  kurtosis("amount")                     // 峰度
).show()
```

> **踩坑经验**：`collect_list` 的结果顺序是不确定的！如果需要保证顺序，需要在窗口函数中先排序再收集。此外，`collect_set`/`collect_list` 在 group 内数据量很大时可能导致 OOM，一定要确认 group 内的数据量。

> **面试点**：`approx_count_distinct` 的误差逻辑是什么？它基于 HyperLogLog 算法，通过第二个参数 `relativeSD` 控制精度，默认 0.05（5% 误差）。设为 0.01 时精度更高但内存占用更大。在 UV 统计等允许一定误差的场景下，用近似函数可以大幅提升性能。

### Join 操作

```scala
val users = Seq((1, "张三"), (2, "李四")).toDF("id", "name")
val orders = Seq((1, "手机", 5999), (1, "电脑", 8999), (2, "耳机", 299)).toDF("uid", "product", "price")

// 内连接
users.join(orders, users("id") === orders("uid"))

// 左外连接
users.join(orders, users("id") === orders("uid"), "left")

// 自定义 Join 类型
users.join(orders, users("id") === orders("uid"), "left_anti")  // 只在左表不在右表

// Broadcast Hint
import org.apache.spark.sql.functions.broadcast
users.join(broadcast(orders), users("id") === orders("uid"))
```

Join 类型一览：

| Join 类型 | 说明 | 适用场景 |
|----------|------|---------|
| inner | 两表匹配的行 | 最常用 |
| left | 左表全部 + 右表匹配 | 保证左表不丢 |
| right | 右表全部 + 左表匹配 | 保证右表不丢 |
| full / full_outer | 两表全部 | 数据合并 |
| left_semi | 在右表**存在**匹配的左表行 | 过滤（类似 EXISTS） |
| left_anti | 在右表**不存在**匹配的左表行 | 反过滤（类似 NOT EXISTS） |
| cross | 笛卡尔积 | 谨慎使用 |

> **面试点**：Broadcast Hint 是什么原理？当小表小于 `spark.sql.autoBroadcastJoinThreshold`（默认 10MB）时，Spark 会自动将小表广播到所有 Executor 的内存中，然后在大表侧做本地 Map 端 Join，避免 Shuffle。使用 `broadcast()` 函数可以手动强制这个行为。手动广播时表的大小没有限制，但如果超出 Executor 内存会导致 OOM。

### 窗口函数

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

val windowSpec = Window.partitionBy("department").orderBy(desc("salary"))

df.withColumn("rn", row_number().over(windowSpec))
  .withColumn("rank", rank().over(windowSpec))
  .withColumn("avg_salary", avg("salary").over(windowSpec))
  .where("rn <= 3")  // 每个部门工资 TOP 3
```

窗口函数的执行逻辑分为三步：

```
1. Partition By：按指定列分组（将数据分成多个分区）
2. Order By：在每个分区内排序
3. Frame：定义窗口范围（默认从分区开始到当前行）
```

> **踩坑经验**：窗口函数一定会触发 Shuffle（如果用了 PartitionBy），因为同组数据需要落到同一个分区。如果分区列有严重的数据倾斜（比如"行政部"有 10000 人，"技术部"只有 5 人），窗口计算会非常慢。这时可以考虑先过滤掉倾斜的组单独处理。

### 缺失值处理

```scala
// 删除包含 null 的行
df.na.drop()
df.na.drop(threshold = 3)  // 至少 3 个非空值才保留
df.na.drop(Seq("age", "name"))  // 指定列有空值才删除

// 填充 null
df.na.fill(0)  // 所有数值列填 0
df.na.fill(Map("age" -> 20, "city" -> "未知"))  // 指定列填充

// 用其他行的值填充
import org.apache.spark.sql.functions._
df.withColumn("age", coalesce($"age", lit(20)))
```

### 去重

```scala
// 全列去重
df.distinct()

// 指定列去重（保留第一个出现的行）
df.dropDuplicates("user_id", "dt")

// 有排序的去重（保留每个 user_id 最新的行）
import org.apache.spark.sql.functions._
df.withColumn("rn", row_number().over(Window.partitionBy("user_id").orderBy(desc("dt"))))
  .where("rn = 1")
  .drop("rn")
```

## 全局优化：Catalyst 优化器

```
SQL / DataFrame API
        │
   ┌────▼────┐
   │ Parser   │  → Unresolved Logical Plan
   └────┬────┘
        │
   ┌────▼─────────┐
   │ Analyzer      │  → Resolved Logical Plan
   │ (Catalog 解析) │
   └────┬─────────┘
        │
   ┌────▼─────────┐
   │ Optimizer     │  → Optimized Logical Plan
   │ (规则优化)     │
   └────┬─────────┘
        │
   ┌────▼─────────┐
   │ Planner       │  → Physical Plan(s)
   │ (成本选择)     │
   └────┬─────────┘
        │
   ┌────▼─────────┐
   │ Code Gen      │  → RDD / Tungsten
   │ (全阶段代码生成)│
   └──────────────┘
```

> **面试点**：Catalyst 优化器是一个基于规则的优化器（RBO），不是基于成本的优化器（CBO）。直到 Spark 3.0+ 引入了一些成本优化特性，但核心仍是规则驱动。这意味着它根据预定义的规则如"谓词下推"、"列裁剪"来优化，而不需要表的统计信息。

### 关键优化规则

| 规则 | 说明 | 效果 |
|------|------|------|
| 谓词下推（Predicate Pushdown） | 将 WHERE 过滤推到数据源（Parquet/ORC 跳过行组） | IO 大幅减少 |
| 列裁剪（Column Pruning） | 只读取 SQL 引用的列 | IO + 内存节省 |
| 常量折叠（Constant Folding） | `1 + 2` 编译为 `3` | 减少计算 |
| Join 重排序 | 按表大小重排 Join 顺序 | 减少中间数据量 |
| 投影合并（Project Collapsing） | 合并连续 select | 减少算子 |
| 分区剪裁（Partition Pruning） | 过滤分区列时只读相关分区 | 跳过无关文件 |
| Null 传播 | 自动优化 nullable 列 | 减少空值检查 |

### 如何查看执行计划

```scala
// 查看逻辑计划
df.explain(true)

// 查看物理计划（更常用）
df.explain()

// 查看具体执行细节
df.queryExecution.debug.toLatex

// 示例输出
// == Physical Plan ==
// *(1) Project [user_id#12L, sum#24L AS total#26L]
// +- *(1) HashAggregate(keys=[user_id#12L], functions=[sum(amount#14L)])
//    +- Exchange hashpartitioning(user_id#12L, 200)
//       +- *(1) HashAggregate(keys=[user_id#12L], functions=[partial_sum(amount#14L)])
//          +- *(1) FileScan parquet [user_id#12L, amount#14L] ...
```

> **踩坑经验**：查看执行计划是调优的第一步。如果你看到 `BroadcastExchange` 的估算大小远大于实际，说明统计信息不准确，可以手动 `ANALYZE TABLE` 更新统计。如果你看到 `Sort` 操作在海量数据上，但查询中并没有 `ORDER BY`，可能是 `ORDER BY` 是分布式排序所需的。

## UDF / UDAF

### UDF（用户自定义函数）

```scala
// 注册 UDF
val toUpperCase = udf((s: String) => s.toUpperCase)
df.select(toUpperCase($"name"))

// Spark SQL 中使用
spark.udf.register("toUpperCase", (s: String) => s.toUpperCase)
spark.sql("SELECT toUpperCase(name) FROM people")
```

> **面试点**：UDF 的问题是 Catalyst 优化器不知道 UDF 内部的逻辑。比如 UDF 内部做了过滤，但 Catalyst 无法做谓词下推，这可能导致性能下降。如果可以用 Spark 内置函数实现，尽量不用 UDF。此外，UDF 的序列化开销比内置函数大，因为每行数据都要经过 Scala lambda 调用。

### UDAF（用户自定义聚合函数）

```scala
import org.apache.spark.sql.expressions.Aggregator

// 自定义聚合器：计算加权平均
case class WeightedValue(value: Double, weight: Double)
case class WeightedSum(sum: Double, weightSum: Double)

object WeightedAverage extends Aggregator[WeightedValue, WeightedSum, Double] {
  def zero: WeightedSum = WeightedSum(0, 0)
  def reduce(b: WeightedSum, a: WeightedValue): WeightedSum =
    WeightedSum(b.sum + a.value * a.weight, b.weightSum + a.weight)
  def merge(b1: WeightedSum, b2: WeightedSum): WeightedSum =
    WeightedSum(b1.sum + b2.sum, b1.weightSum + b2.weightSum)
  def finish(b: WeightedSum): Double = b.sum / b.weightSum
  def bufferEncoder: Encoder[WeightedSum] = Encoders.product
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

val weightedAvg = WeightedAverage.toColumn.name("weighted_avg")
df.select(weightedAvg)
```

UDAF 的执行流程：

```
每个分区内：reduce 合并行 → 跨分区：merge 合并结果 → finish 输出最终结果
```

## 性能调优

### 配置

```scala
// Shuffle 分区数（默认 200，通常太小）
spark.conf.set("spark.sql.shuffle.partitions", 400)

// 自适应执行（Spark 3.0+）
spark.conf.set("spark.sql.adaptive.enabled", true)
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)

// Broadcast Join 阈值（默认 10MB）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 52428800L)  // 50MB
```

### 文件格式与压缩

```scala
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.orc.compression.codec", "snappy")

// 写 Parquet（列式存储 + 压缩）
df.write
  .mode("overwrite")
  .partitionBy("dt")     // 按日期分区
  .bucketBy(128, "uid")  // 按用户分桶
  .sortBy("uid")
  .parquet("hdfs://output")
```

> **踩坑经验**：`bucketBy` + `sortBy` 只在使用 `saveAsTable` 时有效，直接写 Parquet 文件时 `bucketBy` 不会生效。此外，分桶的好处主要体现在 Join 优化上——如果两张表按相同的键分桶，Join 时可以避免 Shuffle。

### 缓存策略

```scala
// 缓存到内存
df.cache()  // 等同于 df.persist(StorageLevel.MEMORY_AND_DISK)
df.persist(StorageLevel.MEMORY_ONLY_SER)  // 序列化缓存，节省内存

// 检查缓存
spark.catalog.isCached("myTable")

// 清除缓存
df.unpersist()
```

| 存储级别 | 空间占用 | CPU 时间 | 是否存磁盘 |
|---------|---------|---------|-----------|
| MEMORY_ONLY | 大 | 少 | 否 |
| MEMORY_ONLY_SER | 小（序列化） | 多（序列化耗时） | 否 |
| MEMORY_AND_DISK | 中 | 少 | 是 |
| MEMORY_AND_DISK_SER | 小 | 多 | 是 |
| DISK_ONLY | 最小 | 最多 | 是 |

## 小结

| 层面 | 核心点 |
|------|--------|
| API | DataFrame = Schema + SQL，Dataset = Schema + TypeSafe |
| Catalyst | 解析→分析→优化→物理计划→代码生成 |
| 存储 | Parquet/ORC 列式 + Snappy 压缩 + 分区剪裁 |
| Join 策略 | Broadcast Hash Join (首选) → Sort Merge Join → Shuffle Hash Join |
| 最佳实践 | 减少 Shuffle、尽早过滤、合理分区、用 Column 表达式 |
| UDF 注意事项 | 每次用 UDF 思考能否用内置函数替代 |
| 缓存 | 中等热数据用 MEMORY_AND_DISK，频繁用的小表广播 |
