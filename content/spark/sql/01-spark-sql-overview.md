# Spark SQL — 概述与核心概念

## 为什么需要 Spark SQL

在 Spark 生态出现之前，大数据领域处理结构化数据主要依赖 Hive。Hive 将 SQL 翻译成 MapReduce 作业，但 MapReduce 的磁盘落地计算模型导致延迟很高。同时，Spark Core 提供的 RDD API 虽然灵活强大，但缺乏 Schema 信息，无法针对结构化数据做深度优化。

这就引出了一个核心问题：**能不能把 SQL 的易用性和 Spark 的内存计算优势结合起来？**

Spark SQL 给出的答案是：不仅可以，还能做得更好。它带来了两个革命性能力：

| 能力 | 解决的问题 | 带来的价值 |
|------|-----------|-----------|
| **统一 SQL 引擎** | 多数据源查询语法不一致 | 用标准 SQL 查 Hive/Parquet/JDBC/Kafka |
| **声明式 API** | RDD 编程门槛高 | DataFrame/Dataset API 自动优化 |
| **Catalyst 优化器** | 用户需手动调优 | 自动谓词下推、列裁剪、Join 重排 |
| **Tungsten 执行引擎** | JVM 对象开销大 | 堆外内存管理 + 全阶段代码生成 |

## Spark SQL 是什么

Spark SQL 是 Spark 生态中处理**结构化数据**的核心模块，支持 SQL 2003 标准和 DataFrame/Dataset API。

```
Spark SQL 两大价值：

1. 统一的 SQL 引擎
   └── 可以用标准 SQL 查询 Hive、Parquet、ORC、JDBC、Kafka 等

2. 编程 API（DataFrame + Dataset）
   └── 在编译器获得 Schema，Catalyst 优化器自动优化
```

> **面试点**：Spark SQL 最核心的设计思想是"统一"。无论是批处理、流处理、还是 ad-hoc 查询，都用同一套 SQL 引擎和优化器，计算引擎和存储格式解耦。

### 发展历程

```
Spark 1.0 — Shark（基于 Hive）
         └── 性能受限（依赖 Hive 解析器）
Spark 1.3 — DataFrame API 引入
         └── 全新的 Catalyst 优化器
Spark 1.6 — Dataset API（实验性）
         └── 类型安全的编码器
Spark 2.0 — SparkSession 统一入口
         └── Dataset 成为主力 API（DataFrame = Dataset[Row]）
Spark 3.0 — AQE + Dynamic Partition Pruning
         └── 自适应查询执行
```

各版本演进的核心驱动力：

| 版本 | 里程碑 | 核心改进 |
|------|--------|---------|
| 1.0 | Shark 发布 | 基于 Hive 的 SQL-on-Spark 方案 |
| 1.3 | DataFrame | 引入 Catalyst 优化器，不再依赖 Hive |
| 1.6 | Dataset (实验) | Encoder 序列化，JVM GC 压力大幅降低 |
| 2.0 | SparkSession | API 统一，DataFrame = Dataset[Row] |
| 3.0 | AQE | 运行时自动优化，解决数据倾斜等痛点 |
| 3.4+ | ANSI SQL | 更强的 SQL 标准兼容性 |

> **面试点**：Spark SQL 和 Hive on Spark 的区别？Spark SQL 是 Spark 原生的 SQL 引擎，使用 Catalyst 优化器；Hive on Spark 只是把 Hive 的执行引擎从 MR 换成 Spark，解析器仍是 Hive 的，性能不如 Spark SQL。

### 核心优势

- **性能**：比 Hive 快 10-100 倍（内存计算 + Catalyst 优化 + Tungsten 代码生成）
- **统一**：一套引擎处理批/流/交互式查询，统一 SQL + API
- **开放**：支持多种数据源（Hive、Parquet、ORC、Avro、JDBC、Kafka 等）
- **集成**：与 MLlib、Structured Streaming、GraphX 无缝集成

> **踩坑经验**：很多初学者以为 Spark SQL 只能查文件，实际上它通过 JDBC DataSource 可以直接查 MySQL/PostgreSQL，不需要先导入数据。这在数据量不大（< 100GB）的场景下非常实用，可以省掉数据导入的步骤。

## SparkSession — 统一入口

Spark 1.x 时代，用户需要根据场景选择不同的入口：

```scala
// Spark 1.x 的混乱局面
val sc = new SparkContext(conf)              // 基础入口
val sqlContext = new SQLContext(sc)          // SQL 入口
val hiveContext = new HiveContext(sc)        // Hive 入口
val streamingContext = new StreamingContext(sc) // 流处理入口
```

每个 Context 都有自己的配置和行为，用户经常搞混该用哪个。Spark 2.0 统一为 SparkSession：

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("Spark SQL Demo")
  .config("spark.sql.adaptive.enabled", "true")
  .config("spark.sql.shuffle.partitions", "200")
  .enableHiveSupport()
  .getOrCreate()

// SparkSession 同时提供：
// 1. spark.read     — DataFrameReader
// 2. spark.sql      — SQL 查询入口
// 3. spark.table    — 直接读取表
// 4. spark.catalog  — 元数据操作
```

SparkSession 的常用配置项：

| 配置项 | 默认值 | 说明 | 建议 |
|--------|--------|------|------|
| `spark.sql.shuffle.partitions` | 200 | Shuffle 时分区数 | 大集群设 400-800 |
| `spark.sql.adaptive.enabled` | true (3.0+) | 自适应查询执行 | 保持开启 |
| `spark.sql.autoBroadcastJoinThreshold` | 10MB | Broadcast Join 阈值 | 可适当增大 |
| `spark.sql.adaptive.coalescePartitions.enabled` | true (3.2+) | 合并小分区 | 保持开启 |

> **踩坑经验**：构建 SparkSession 时如果多次调用 `getOrCreate()`，只有第一次的配置生效。如果需要不同配置，需要用 `spark.newSession()` 创建新的 Session。

## DataFrame 和 Dataset

### DataFrame

DataFrame 是**有 Schema 的 RDD**，每行是 `Row` 类型。它比 RDD 多了两层：
1. **Schema 信息**：知道每列的名称和类型
2. **优化机会**：Catalyst 可以根据 Schema 做深度优化

```scala
val df = spark.read.parquet("hdfs://data/orders")
df.printSchema()
// root
//  |-- order_id: long (nullable = true)
//  |-- user_id: long (nullable = true)
//  |-- amount: double (nullable = true)
//  |-- dt: string (nullable = true)

df.show(5)
// +--------+-------+------+----------+
// |order_id|user_id|amount|        dt|
// +--------+-------+------+----------+
// |       1|    101| 99.90|2024-01-01|
// |       2|    102|199.00|2024-01-01|
// +--------+-------+------+----------+
```

### Dataset

Dataset 是**类型安全**的 DataFrame，每行是一个自定义类型。它在 DataFrame 的基础上增加了编译期类型检查。

```scala
case class Order(order_id: Long, user_id: Long, amount: Double, dt: String)

val ds: Dataset[Order] = spark.read.parquet("hdfs://data/orders").as[Order]

// 编译期类型安全
ds.filter(_.amount > 100).show()
// .amount 是 Double 类型，IDE 和编译器都能检查
```

Dataset 的 Encoder 机制是它和 DataFrame 的本质区别：

| 特性 | DataFrame (Dataset[Row]) | Dataset[T] |
|------|------------------------|------------|
| 类型信息 | 运行时 Row | 编译期 Case Class |
| 序列化 | Tungsten Row | Encoder[T] |
| API 风格 | SQL / Column 表达式 | 函数式 |
| 适用场景 | 数据分析和 ETL | 复杂业务逻辑 |
| 类型安全 | 运行时检查 | 编译期检查 |
| 性能 | 高（无反射） | 略低（Encoder 开销） |

### DataFrame vs Dataset

```scala
// DataFrame = Dataset[Row]（Spark 2.0+ 定义）
// 所以：
// df.isInstanceOf[Dataset[_]]  // true

// Schema 检查时机不同
// DataFrame：运行时检查列名/类型
df.select("non_exist_column")  // 运行时报错

// Dataset：编译期检查
ds.filter(_.nonExistField > 0)  // 编译期报错！
```

> **面试点**：什么时候用 DataFrame 什么时候用 Dataset？
> - **DataFrame** 适合：SQL 分析、Schema 未知的数据、Python 用户
> - **Dataset** 适合：需要编译期类型检查、复杂业务逻辑的 Scala/Java 用户
> - 实践中大部分场景用 DataFrame 即可，只有在需要类型安全的复杂管道时才用 Dataset

### RDD vs DataFrame vs Dataset 对比

| 维度 | RDD | DataFrame | Dataset[T] |
|------|-----|-----------|------------|
| Schema | 无 | 有 | 有 |
| 类型安全 | 编译期（泛型） | 运行时 | 编译期 |
| 优化器 | 无（用户手动） | Catalyst | Catalyst |
| 序列化 | Java/Kryo | Tungsten Row | Encoder |
| API | 函数式 | 声明式 SQL | 函数式 + SQL |
| GC 影响 | 大（对象多） | 小（堆外） | 小 |
| 数据源 | 无 Schema | 丰富 | 丰富 |
| Python 支持 | 是 | 是 | 否（仅 Scala/Java） |

## SQL vs DataFrame API

两种写法的执行计划完全一样（都经过 Catalyst 优化器）。

```scala
// SQL 写法
spark.sql("SELECT user_id, SUM(amount) as total FROM orders WHERE amount > 0 GROUP BY user_id")

// DataFrame API 写法
df.filter($"amount" > 0)
  .groupBy("user_id")
  .agg(sum("amount").alias("total"))
```

> 两种方式生成的执行计划完全一致。选择标准：团队技术水平（SQL 门槛低）vs 代码可维护性（API 可组合）。

DataFrame API 相比 SQL 的优势：

| 维度 | SQL 写法 | DataFrame API |
|------|---------|---------------|
| 学习成本 | 低（人人会 SQL） | 中（需学 DSL） |
| 代码组织 | 字符串拼接，不易测试 | 函数组合，IDE 友好 |
| 动态逻辑 | 拼 SQL 字符串（危险） | if/else 自然处理 |
| 复杂度 | 简单查询简洁 | 复杂逻辑颗粒度可控 |
| 复用性 | CTE/子查询 | 函数/变量自然复用 |

> **踩坑经验**：SQL 字符串拼接是 SQL 注入的重灾区。如果查询条件来自外部输入，用 DataFrame API 的 `filter` 和 `when/otherwise` 可以避免 SQL 注入风险。

## Catalog

Spark SQL 的 Catalog 管理元数据。它提供了统一的元数据访问接口，无论底层是 Hive Metastore 还是内存中的视图。

```scala
// 列出所有数据库
spark.catalog.listDatabases().show()

// 列出当前库的表
spark.catalog.listTables().show()

// 列出表的列
spark.catalog.listColumns("orders").show()

// 缓存表（适合多次查询的小表）
spark.catalog.cacheTable("orders")
spark.catalog.uncacheTable("orders")
```

> **踩坑经验**：`cacheTable` 缓存的是整个表的数据，消耗内存。如果表很大，应该用 Spark SQL cache 的 `SELECT` + `WHERE` 来只缓存需要的部分：`spark.sql("CACHE TABLE cached_orders AS SELECT * FROM orders WHERE dt = '2024-01-01'")`。

## 执行流程

Spark SQL 的查询执行经历了一条完整的管道：

```
SQL/DataFrame API
    │
    ▼
Parser（SQL 解析）────► Unresolved Logical Plan
    │                       │ (表名、列名未验证)
    ▼
Analyzer（语义分析）───► Resolved Logical Plan
    │                       │ (连接 Catalog 校验)
    ▼
Optimizer（规则优化）───► Optimized Logical Plan
    │                       │ (谓词下推、列裁剪等)
    ▼
Planner（物理计划）───► Physical Plan(s)
    │                       │ (可能有多个候选)
    ▼
Cost Model（成本选择）──► Selected Physical Plan
    │
    ▼
Code Generation（代码生成）──► RDD[Tungsten]
```

> **面试点**：Catalyst 优化器最核心的三个阶段是 Analyzer（校验 Schema）、Optimizer（逻辑优化）、Planner（物理计划选择）。面试中常问的谓词下推和列裁剪发生在 Optimizer 阶段。

每个阶段的作用：

| 阶段 | 输入 → 输出 | 做了什么 |
|------|------------|---------|
| Parser | SQL 字符串 → Unresolved Logical Plan | SQL 语法解析，生成抽象语法树 |
| Analyzer | Unresolved Plan → Resolved Plan | 连接 Catalog 解析表名、列名类型 |
| Optimizer | Resolved Plan → Optimized Plan | 谓词下推、列裁剪、常量折叠等 60+ 规则 |
| Planner | Optimized Plan → Physical Plans | 生成多个候选物理执行计划 |
| Cost Model | Plans → Selected Plan | 基于统计信息选择最优物理计划 |
| Code Gen | Selected Plan → Java Code | 全阶段代码生成（WholeStageCodegen） |

## 面试高频考点

### Q: 什么时候用 DataFrame 什么时候用 Dataset？

DataFrame 适合：SQL 分析、Schema 未知的数据、Python 用户。Dataset 适合：需要编译期类型检查、复杂业务逻辑的 Scala/Java 用户。实践中大部分场景用 DataFrame 即可。

### Q: Spark 2.0 统一入口为什么是 SparkSession？

Spark 1.x 有 SQLContext、HiveContext、StreamingContext 等多个入口，用户容易混淆。Spark 2.0 统一为 SparkSession，降低使用门槛。同时 SparkSession 内置了 SparkContext、SQLContext、StreamingContext 等，一个入口搞定所有场景。

### Q: Spark SQL 的优势？

1. 比 Hive 快（内存计算 + Catalyst 优化 + 代码生成）
2. 统一处理各种数据源（Hive、Parquet、JDBC、Kafka 等）
3. SQL + DataFrame API 两套接口
4. 与其他 Spark 组件无缝集成

### Q: Catalyst 优化器的三个阶段是什么？

Analyzer：校验表名列名是否存在，解析数据类型。Optimizer：逻辑优化，执行谓词下推、列裁剪等规则。Planner：生成物理计划并选择代价最小的。

### Q: 什么是全阶段代码生成？

WholeStageCodegen（全阶段代码生成）将多个物理算子编译成一个 Java 函数，消除了虚函数调用和中间数据的物化开销。这是 Spark SQL 比 RDD API 快 2-5 倍的关键原因之一。

## 小结

| 概念 | 要点 |
|------|------|
| SparkSession | 2.0+ 统一入口，内置 Catalog、Streaming 等能力 |
| DataFrame | 有 Schema 的分布式数据集，运行时类型，适合 Python/SQL 用户 |
| Dataset | 编译期类型安全的 DataFrame，适合 Scala/Java 复杂业务 |
| SQL vs API | 执行计划相同，选择看团队，API 更安全但 SQL 门槛低 |
| Catalyst | Spark SQL 的查询优化器核心，60+ 优化规则 |
| WholeStageCodegen | 全阶段代码生成，消除虚函数调用，性能关键 |
| AQE | 3.0+ 自适应执行，运行时根据统计动态优化 |
