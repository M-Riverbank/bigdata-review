# Spark SQL — 基础操作

## 为什么需要掌握 Spark SQL 基础操作

在日常大数据开发中，80% 以上的工作都是围绕数据的读取、转换和写入展开的。Spark SQL 提供了两套风格迥异但殊途同归的操作方式：**SQL 字符串**和 **DataFrame API**。

- 如果你是数据分析师，习惯了写 SQL，可以直接用 `spark.sql("SELECT ...")`
- 如果你是后端转大数据的开发，DataFrame API 的链式调用让你感觉像在使用 Java Stream 或 Kotlin 集合操作
- 两者在 Catalyst 优化后会生成**完全相同的执行计划**

> **面试点**：面试官常问"你们项目用 SQL 还是 DataFrame API？"这不是在选技术，而是在考察你对团队管理和代码可维护性的思考。建议回答：数据量小、临时分析用 SQL；生产 ETL 用 API 利于单元测试和代码复用。

## 读取数据

### DataFrameReader

Spark SQL 通过 `DataFrameReader` 提供了统一的读数据入口。

```scala
// 统一入口
val df = spark.read.format("parquet").load("hdfs://data/table")
```

`DataFrameReader` 的核心设计思想是："格式 + 选项 + 路径"三板斧。无论底层是什么格式，读取方式都是一致的。

### 各格式读取

```scala
// Parquet（Spark 原生格式，读取速度最快）
val parquetDF = spark.read.parquet("hdfs://data/orders")

// ORC（Hive 原生格式）
val orcDF = spark.read.orc("hdfs://data/orders")

// JSON（半结构化，自动推断 Schema）
val jsonDF = spark.read.json("hdfs://data/logs/*.json")

// CSV（最常用，需指定分隔符）
val csvDF = spark.read
  .option("header", "true")
  .option("sep", ",")
  .option("inferSchema", "true")
  .csv("hdfs://data/orders.csv")

// JDBC（关系数据库）
val jdbcDF = spark.read
  .option("url", "jdbc:mysql://host:3306/db")
  .option("dbtable", "orders")
  .option("user", "xxx")
  .option("password", "xxx")
  .format("jdbc").load()

// 从 Hive 表读取
val hiveDF = spark.table("default.orders")
spark.sql("SELECT * FROM default.orders")
```

| 格式 | 优点 | 缺点 | 推荐场景 |
|------|------|------|---------|
| Parquet | 列式存储，压缩率高，Schema 内嵌 | 二进制不可读 | **首选格式** |
| ORC | 比 Parquet 压缩率更高（Hive 场景） | Hive 生态绑定 | Hive 表 |
| JSON | 可读，Schema 灵活 | 解析慢，无索引 | 日志采集、API 数据 |
| CSV | 通用，Excel 可打开 | 无 Schema，类型全字符串 | 数据交换 |
| JDBC | 直接连接数据库 | 性能受限，不能并行读大表 | ODS 层同步 |

> **踩坑经验**：JDBC 读取大表（超 1000 万行）时，单分区读取会导致 Driver OOM。必须设置分区字段才能并行读取：
> ```scala
> val jdbcDF = spark.read
>   .option("dbtable", "orders")
>   .option("partitionColumn", "id")           // 分区列
>   .option("lowerBound", "1")                 // 下界
>   .option("upperBound", "10000000")          // 上界
>   .option("numPartitions", "20")            // 分区数
>   .format("jdbc").load()
> ```

### Schema 推断

```scala
// CSV/JSON 可以不手动指定 Schema（Spark 自动扫描前 1000 行推断）
// 推荐：大数据量下手动指定 Schema（避免扫描推断的开销）

import org.apache.spark.sql.types._

val schema = StructType(Array(
  StructField("order_id", LongType, nullable = false),
  StructField("user_id", LongType, nullable = true),
  StructField("amount", DoubleType, nullable = true),
  StructField("dt", StringType, nullable = true)
))

val orders = spark.read
  .schema(schema)
  .option("header", "true")
  .csv("hdfs://data/orders.csv")
```

**Schema 推断 vs 手动指定对比：**

| 维度 | Schema 推断 | 手动指定 |
|------|-----------|---------|
| 启动时间 | 慢（需额外扫描任务） | 快（直接读取） |
| 准确性 | 可能误判（如 ID 推断为 Int 导致溢出） | 精确控制 |
| 代码量 | 少 | 多 |
| 推荐场景 | 探索性分析、Schema 频繁变动的数据 | 生产 ETL、已知 Schema |

> **踩坑经验**：Schema 推断可能出现"类型误判"。例如一个只有 1-100 的订单 ID 列，Spark 会推断为 IntegerType，但如果将来出现超过 2^31 的 ID，会导致数据损坏。生产环境中**永远手动指定 Schema**。

### CSV 常用配置

```scala
val df = spark.read
  .option("header", "true")           // 第一行为列名
  .option("sep", "|")                 // 分隔符（默认逗号）
  .option("inferSchema", "false")     // 不推断 Schema
  .option("nullValue", "")            // 空值表示
  .option("dateFormat", "yyyy-MM-dd") // 日期格式
  .option("maxColumns", "2048")       // 最大列数
  .option("mode", "PERMISSIVE")       // 解析模式：PERMISSIVE/DROPMALFORMED/FAILFAST
  .schema(schema)
  .csv("path/*.csv")
```

## 基础转换

### 选择列

```scala
// 方式 1：字符串列名
df.select("user_id", "amount")

// 方式 2：Column 表达式（更灵活）
import org.apache.spark.sql.functions._
df.select($"user_id", $"amount" * 1.1, $"dt")

// 方式 3：SQL 表达式字符串
df.selectExpr("user_id", "amount * 1.1 as tax_amount", "dt")

// 方式 4：正则选择列
df.select(col("order_id"), colRegex("`user_.*`"))
```

> **面试点**：`$"col"`、`col("col")`、`df("col")` 三种方式的区别？`$"col"` 是 Scala 隐式转换的语法糖；`col("col")` 是函数式创建 Column；`df("col")` 从指定 DataFrame 获取列。三者都生成 Column 对象，但 `df("col")` 会绑定到具体 DataFrame，在其他 DataFrame 使用时可能出错。

### 过滤

```scala
// 方式 1：Column 表达式
df.filter($"amount" > 100)
df.filter($"amount" > 100 && $"dt" === "2024-01-01")

// 方式 2：SQL 表达式
df.filter("amount > 100")

// 方式 3：多个 filter 链式调用（Catalyst 会合并）
df.filter("amount > 100").filter("dt = '2024-01-01'")
```

> **踩坑经验**：等值判断用 `===` 而不是 `==`！`===` 是 Column 的等值判断方法，返回 Column 类型。`==` 是 Scala 的对象引用比较，使用 `==` 会直接返回 `false`，而且代码还能通过编译，非常坑人。

常见过滤条件写法：

| 需求 | 写法 |
|------|------|
| 等值判断 | `$"col" === "value"` |
| 不等判断 | `$"col" =!= "value"` |
| 范围判断 | `$"col" > 100 && $"col" < 200` |
| IN 判断 | `$"city".isin("北京", "上海")` |
| 空值判断 | `$"col".isNull` |
| LIKE | `$"name".like("张%")` |
| 正则 | `$"name".rlike("^张.*")` |
| 条件组合 | `($"age" > 18) || ($"city" === "北京")` |

### 新增列

```scala
// withColumn — 新增或替换列
df.withColumn("tax", $"amount" * 0.1)
df.withColumn("amount_plus_tax", $"amount" + $"tax")
df.withColumn("dt_date", to_date($"dt", "yyyy-MM-dd"))

// withColumnRenamed — 重命名
df.withColumnRenamed("order_id", "oid")

// 新增常量列
df.withColumn("source", lit("hdfs"))

// 条件列
import org.apache.spark.sql.functions._
df.withColumn("level", 
  when($"amount" > 1000, "VIP")
    .when($"amount" > 100, "普通")
    .otherwise("新用户")
)

// 复杂类型操作
df.withColumn("first_product", $"products"(0))  // 取数组第一个元素
df.withColumn("product_count", size($"products"))
df.withColumn("product_set", array_distinct($"products"))
```

> **踩坑经验**：`withColumn` 每次调用都会生成一个新的逻辑计划节点。如果连续调用了 50 次 `withColumn`，执行计划树会变得非常深，Catalyst 虽然能合并但仍有开销。性能敏感场景下，推荐用 `select` 一次性处理所有列：
> ```scala
> df.select($"*", 
>   ($"amount" * 0.1).as("tax"),
>   to_date($"dt", "yyyy-MM-dd").as("dt_date")
> )
> ```

### 聚合

```scala
// groupBy + agg
df.groupBy("user_id").agg(
  count("*").alias("order_count"),
  sum("amount").alias("total_amount"),
  avg("amount").alias("avg_amount"),
  max("amount").alias("max_amount")
)

// 多种聚合函数
import org.apache.spark.sql.functions._
df.agg(
  countDistinct("user_id"),
  approx_count_distinct("user_id", 0.01),  // 近似去重（HyperLogLog）
  collect_set("category"),                  // 收集去重列表
  collect_list("category")                  // 收集列表
)
```

常用聚合函数一览：

| 函数 | 说明 | 注意事项 |
|------|------|---------|
| `count` | 计数（含 null） | `count(*)` 和 `count(col)` 不同 |
| `sum` | 求和 | 非数值列报错 |
| `avg` / `mean` | 平均值 | 自动忽略 null |
| `stddev` | 标准差 | 描述数据分布 |
| `skewness` | 偏度 | 判断数据是否对称 |
| `kurtosis` | 峰度 | 判断数据是否集中 |
| `first` / `last` | 首/尾值 | **无排序保证！** |
| `collect_list` | 收集为列表 | 可能 OOM |
| `approx_count_distinct` | 近似去重 | 性能远优于 countDistinct |

### 排序

```scala
// 单列排序
df.orderBy("dt")

// 多列排序
df.orderBy($"dt".desc, $"amount".asc)

// sortWithinPartitions — 分区间不排序，分区内排序（比 orderBy 快）
df.sortWithinPartitions($"amount".desc)
```

> **面试点**：`orderBy` 和 `sortWithinPartitions` 的区别？`orderBy` 是全量排序，会触发一次 Shuffle 做全局排序。`sortWithinPartitions` 只保证分区内有序，分区间的数据是无序的。如果后续操作是写入分区表（比如按 dt 分区），用 `sortWithinPartitions` 可以避免一次 Shuffle。

### 分区操作

```scala
// 重分区（触发 Shuffle）
df.repartition(100)                    // 按 Hash 重分区
df.repartition($"dt")                  // 按列重分区
df.repartition(100, $"dt")             // 按列重分区 + 指定分区数

// 合并分区（避免 Shuffle，只合并小文件）
df.coalesce(50)                        // 只能减少分区数，不能增加
```

## SQL 写法

```scala
// 注册临时视图
df.createOrReplaceTempView("orders")
df.createOrReplaceGlobalTempView("orders_global")  // 跨 Session

// SQL 查询
spark.sql("""
  SELECT
    user_id,
    COUNT(*) as order_count,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount
  FROM orders
  WHERE amount > 0
  GROUP BY user_id
  HAVING order_count >= 5
  ORDER BY total_amount DESC
""").show()
```

临时视图的对比：

| 视图类型 | 生命周期 | 跨 Session | 适用场景 |
|---------|---------|-----------|---------|
| TempView | SparkSession 内 | 否 | 常规查询 |
| GlobalTempView | SparkContext 生命周期 | 是 | 多 Session 共享数据 |

## 写数据

```scala
// 写入 Parquet（推荐）
df.write.mode("overwrite").parquet("hdfs://output/orders")

// 写入分区表
df.write
  .mode("overwrite")
  .partitionBy("dt")
  .parquet("hdfs://output/orders")

// 写入 Hive 表
df.write
  .mode("overwrite")
  .saveAsTable("default.orders")

// 写入 JDBC
df.write
  .mode("append")
  .option("url", "jdbc:mysql://host:3306/db")
  .option("dbtable", "orders")
  .format("jdbc").save()
```

### SaveMode

| Mode | 说明 | 适用场景 |
|------|------|---------|
| `append` | 追加到已有数据 | 增量写入 |
| `overwrite` | 覆盖已有数据 | 全量刷新 |
| `errorIfExists` | 已存在则报错（默认） | 防止误覆盖 |
| `ignore` | 已存在则跳过 | 幂等写入 |

### 写入选项

```scala
// 控制输出文件
df.write
  .mode("overwrite")
  .option("compression", "snappy")     // 压缩格式
  .option("maxRecordsPerFile", 1000000) // 单个文件最大行数（避免小文件）
  .partitionBy("dt")
  .bucketBy(128, "uid")
  .sortBy("uid")
  .parquet("hdfs://output/orders")
```

> **踩坑经验**：Spark 写入时每个分区会生成一个文件（如果文件太大可能分裂成多个）。这是一个常见的**小文件问题**来源。如果每天的数据量很小（几百 MB），每写入一个分区就产生一个新的小文件，日积月累会导致 NameNode 压力巨大。解决方法：
> 1. 用 `coalesce(n)` 控制输出文件数
> 2. 用 `maxRecordsPerFile` 控制单文件大小
> 3. 定时做小文件合并

## 面试高频考点

### Q: DataFrame 的 filter 和 where 有什么区别？

没有区别。`where` 是 `filter` 的别名，底层调用的是同一个方法。

### Q: DataFrame 和 SQL 哪个性能更好？

性能完全一样——都经过 Catalyst 优化器生成相同的执行计划。选择取决于团队的技能栈。生产环境一般建议 SQL 写临时分析，API 写生产 ETL。

### Q: DataFrame 的 action 和 transformation 是什么？

和 RDD 一样：`transformations` 惰性求值（select、filter、groupBy），`actions` 触发计算（show、collect、write）。理解这个区别对排查性能问题至关重要。

### Q: 什么时候用 coalesce 什么时候用 repartition？

`coalesce` 只能减少分区数，不会触发 Shuffle（只是合并已有的分区）。`repartition` 可以增加或减少分区数，但会触发一次 Shuffle。所以减少分区优先用 `coalesce`，需要增加分区数时只能用 `repartition`。

### Q: CSV 读取时 inferSchema 有什么风险？

除了性能开销外，最大的风险是类型误判。比如 ID 列刚好 1-100 会被推断为 Int，但实际业务中 ID 可能是 Long 甚至 String。生产环境永远手动指定 Schema。

## 小结

| 操作 | 常用方法 | 注意点 |
|------|---------|-------|
| 读取 | spark.read.parquet/json/csv/jdbc | 生产手动指定 Schema |
| 选择 | select / selectExpr / withColumn | 多列用 select，单列新增用 withColumn |
| 过滤 | filter / where | 等值用 `===`，不是 `==` |
| 聚合 | groupBy + agg | big group 小心 OOM |
| 排序 | orderBy / sortWithinPartitions | 分区内排序用 sortWithinPartitions |
| 写入 | write.mode().parquet/saveAsTable | 注意小文件问题 |
| 临时表 | createOrReplaceTempView | 视图不代表数据被缓存 |
