# Spark SQL DataFrame API 深入

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
| 类型安全 | 编译期 | 运行时 | 编译期 |
| 优化器 | 无 | Catalyst | Catalyst |
| 序列化 | Java/Kryo | Tungsten | Encoder |
| API | 函数式 | 声明式 SQL | 函数式 + SQL |
| GC 影响 | 大（对象） | 小（堆外） | 小 |

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
```

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

### 关键优化规则

| 规则 | 说明 |
|------|------|
| 谓词下推（Predicate Pushdown） | 将 WHERE 过滤推到数据源（Parquet/ORC 跳过行组） |
| 列裁剪（Column Pruning） | 只读取 SQL 引用的列 |
| 常量折叠（Constant Folding） | `1 + 2` 编译为 `3` |
| Join 重排序 | 按表大小重排 Join 顺序 |
| 投影合并（Project Collapsing） | 合并连续 select |

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

## 面试高频考点

### Q: Spark SQL 中 Join 策略有哪些？

1. **Broadcast Hash Join**：小表广播到所有 Executor 内存，Map 端完成 Join（最佳）
2. **Shuffle Hash Join**：两表按 key 重分区后 Hash Join
3. **Sort Merge Join**：两表排序后归并（两个大表的默认选择）
4. **Broadcast Nested Loop Join**：笛卡尔积（最差，通常不推荐）

### Q: 为什么 Spark 默认使用 Sort Merge Join？

Sort Merge Join 不需要一张表完全放入内存，只需要排序后的数据流式归并。在大数据量下最稳定，不会 OOM。

### Q: GroupBy 在 Spark SQL 中如何优化？

1. Map 端预聚合（Partial Aggregation）：每个分区先在本地聚合
2. 减少 Shuffle 数据量：预聚合后的数据量远小于原始数据
3. 自适应执行：根据 Shuffle 数据量动态合并分区

### Q: DataFrame cache 的时机？

- 某个 DataFrame 被多次 Action 使用
- 经过昂贵 Shuffle 后需要反复使用的中间结果
- 迭代算法中反复使用的数据

## 小结

| 层面 | 核心点 |
|------|--------|
| API | DataFrame = Schema + SQL，Dataset = Schema + TypeSafe |
| Catalyst | 解析→分析→优化→物理计划→代码生成 |
| 存储 | Parquet/ORC 列式 + Snappy 压缩 + 分区剪裁 |
| Join 策略 | Broadcast Hash Join (首选) → Sort Merge Join → Shuffle Hash Join |
| 最佳实践 | 减少 Shuffle、尽早过滤、合理分区、用 Column 表达式 |
