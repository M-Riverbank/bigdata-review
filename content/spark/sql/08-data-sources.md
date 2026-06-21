# Spark SQL — 数据源与文件格式

## 为什么需要了解数据源

在大数据开发中，数据的"读"和"写"占据了一半以上的工作量。选择正确的文件格式和存储策略，对性能的影响可能比 SQL 优化还要大。

想象一下：
- Parquet 格式下 `SELECT *` 不影响性能（列裁剪），但 CSV 格式下每列都要读
- 分区表下按分区列过滤只扫 1/365 的数据，不分区则要扫全部
- 分桶表 Join 可以避免 Shuffle，而大表 Join 需要全量重分区

> **面试点**：面试官问"你们用 Parquet 还是 ORC？"不是在让你选格式，而是在考察你对列式存储、压缩率、Schema 演进、生态兼容性的理解。回答应该是"看场景..."而不是"我们只用 X"。

## Spark SQL 支持的数据源

统一接口：`DataFrameReader` / `DataFrameWriter`

```scala
// 所有格式使用同一套 API
spark.read.format("parquet").load("path")
spark.read.format("json").load("path")
spark.read.format("csv").load("path")
```

Spark SQL 的核心数据源设计理念是"统一接口 + 插件化实现"。无论是 Parquet、ORC、CSV、JSON、Avro，还是 JDBC 数据库，都通过同样的 `DataFrameReader/Writer` API 操作。

## 文件格式对比

### Parquet（Spark 默认列式格式）

Parquet 是 Spark 生态中最重要的文件格式，也是 Sparks 的**默认存储格式**。

```scala
// Parquet 是 Spark 的首选存储格式
// 特点：列式存储 + 压缩 + Schema 内置 + 谓词下推

// 写入
df.write
  .mode("overwrite")
  .partitionBy("dt", "region")
  .option("compression", "snappy")
  .parquet("hdfs://data/orders")

// 读取
spark.read.parquet("hdfs://data/orders/*.parquet")

// 读取特定分区（分区剪裁自动生效）
spark.read.parquet("hdfs://data/orders/dt=2024-01-01/")
```

Parquet 的四大特性：

| 特性 | 说明 | 性能影响 |
|------|------|---------|
| 列式存储 | 同列数据连续存储 | 只读需要的列，IO 大幅减少 |
| 谓词下推 | RowGroup 级别 min/max 统计 | 跳过不满足条件的行组 |
| Schema 内嵌 | 文件自带 Schema 信息 | 无需外部元数据 |
| 多压缩算法 | Snappy/Zstd/Gzip | 平衡压缩率和速度 |

### ORC（Hive 原生列式格式）

```scala
// ORC 比 Parquet 压缩率更高，但 Spark 支持不如 Parquet 好
df.write.orc("hdfs://data/orders")
spark.read.orc("hdfs://data/orders")
```

### Parquet vs ORC 对比

| 维度 | Parquet | ORC |
|------|---------|-----|
| 压缩率 | 中等 | **更高**（平均差 10~20%） |
| 读性能 | **快**（Spark 原生优化好） | 中等 |
| 写性能 | 中等 | 中等 |
| Schema 演进 | **支持** | 有限支持 |
| 谓词下推 | **支持**（RowGroup min/max） | 支持（Stripe 统计） |
| Hive 集成 | 兼容 | **最佳** |
| Spark 支持 | **最佳**（默认格式） | 好 |
| ACID 事务 | 不支持 | Hive ACID 支持 |

> Spark 优先推荐 Parquet。如果 Hive 表已经是 ORC，Spark 也可以直接读取。

### 四种文件格式全对比

| 维度 | Parquet | ORC | Avro | JSON | CSV |
|------|---------|-----|------|------|-----|
| 存储方式 | 列式 | 列式 | 行式 | 行式 | 行式 |
| 压缩率 | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐ | ⭐⭐ |
| 读性能 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| Schema 演进 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐ |
| 谓词下推 | ✅ | ✅ | ❌ | ❌ | ❌ |
| 人类可读 | ❌ | ❌ | ❌ | ✅ | ✅ |
| 序列化速度 | 快 | 快 | 极快 | 慢 | 中等 |
| 适用场景 | 数仓/ETL | Hive 数仓 | 流式/消息 | 日志/调试 | 数据交换 |

### 其他格式

```scala
// Avro — 行式存储，适合写密集型场景
spark.read.format("avro").load("path")

// JSON — 半结构化，方便调试，但大文件不推荐
spark.read.json("hdfs://data/logs/*.json")

// CSV — 最通用，和其他工具交换数据
spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("hdfs://data/orders.csv")
```

> **面试点**：Avro 和 Parquet 的选择逻辑？**读多写少用 Parquet，写多读少用 Avro**。Avro 是行式存储，写入一条记录的开销很小（不需要重组为列存），适合 Kafka 消息、日志采集等场景。Parquet 是列式存储，写入时需要将行数据重构为列存，写入开销更大但读取时性能更好。

## 分区表

分区是 Hive/Spark SQL 中最重要的性能优化手段之一。它的核心思想是**将数据按列值拆分到不同的目录**，查询时通过目录跳过无关数据。

### 写分区表

```scala
// 按 dt 列分区（Hive 风格目录）
df.write
  .mode("overwrite")
  .partitionBy("dt")
  .parquet("hdfs://data/orders")

// 写入后的目录结构：
// hdfs://data/orders/
//   dt=2024-01-01/
//     part-00001.snappy.parquet
//   dt=2024-01-02/
//     part-00002.snappy.parquet
```

### 读分区表

```scala
// 自动分区剪裁
// 只读取 dt=2024-01-01 目录下的文件
spark.read.parquet("hdfs://data/orders/dt=2024-01-01/")

// 不指定目录：通过 where 过滤，Catalyst 自动剪裁
spark.read.parquet("hdfs://data/orders/")
  .filter($"dt" === "2024-01-01")
  // Catalyst 自动推到数据源级别
```

### 分区剪裁

```
Spark 读取分区表时：
  1. 检查 WHERE 条件是否包含分区列
  2. 跳过不符合条件的分区目录
  3. 只读匹配目录下的文件

效果：1 个月数据（30 天），只查 1 天
  → 只读 1/30 的文件（减少 97% IO！）
```

### 分区策略建议

| 分区策略 | 适合场景 | 注意 |
|---------|---------|------|
| 按天分区 | 日志类、时间序列数据 | 每天数据量适中（100MB+） |
| 按小时分区 | 高频日志 | 小文件问题严重，需合并 |
| 按地域分区 | 按地域过滤 | 地域分布要均衡 |
| 多级分区 | 高频过滤含多列 | 目录层级过深影响性能 |

> **踩坑经验**：分区列的值域过大会导致小文件问题。比如按 `user_id` 分区，每个用户一个分区目录，会有成百上千万个小目录。HDFS 的 NameNode 内存中每个目录/文件大约占 150 字节，1000 万个分区目录就需要 1.5GB 内存。分区列的基数建议控制在 **几百到几千** 之间。

## 分桶表（Bucket Table）

分区解决了"按值过滤"的问题，而分桶解决的是"高频 Join 和聚合"的问题。

```scala
// 分桶 = 按列 Hash 到固定数量文件
// 适合经常 Join 或聚合的中间表

df.write
  .mode("overwrite")
  .bucketBy(128, "user_id")
  .sortBy("user_id")
  .parquet("hdfs://data/orders_bucketed")

// 分桶表的优势：
// 如果两个表按相同的列分桶（桶数相同或倍数关系）
// Join 时无需 Shuffle → Bucket Join
```

> **面试点**：分桶的原理是对分桶列做 Hash，然后将相同 Hash 值的数据写入同一个桶文件。如果 A 表和 B 表都按 user_id 分 128 个桶，那么 user_id=1 的数据一定在 A 的桶 0 和 B 的桶 0，Join 时两个桶 0 在同一个 Executor 上，不需要 Shuffle。

### Bucket Join

```
表 A（按 user_id 分 128 桶）
表 B（按 user_id 分 128 桶）

Join A.id = B.id:
  A 的桶 0 只和 B 的桶 0 连接
  A 的桶 1 只和 B 的桶 1 连接
  ...
  → 无需 Shuffle！数据已经按 Join key 分布好了
```

分区和分桶的对比：

| 维度 | 分区 | 分桶 |
|------|------|------|
| 原理 | 按列值分目录 | 按列 Hash 分文件 |
| 数据分布 | 按值（离散） | 按 Hash（均匀） |
| 适合场景 | 按分区列过滤 | 按分桶列 Join/聚合 |
| 性能提升 | 分区剪裁跳过目录 | 消除 Shuffle |
| 常见问题 | 小文件、数据不均 | 桶数固定后难调整 |
| 列选择 | 基数适中（几百） | 任意列（均匀分布） |

## JDBC 数据源

JDBC 数据源允许 Spark 直接查询关系型数据库，不需要先导入数据。

```scala
// 从关系数据库读取
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://host:3306/db")
  .option("dbtable", "orders")
  .option("user", "xxx")
  .option("password", "xxx")
  .option("numPartitions", "10")     // 并行读取分区数
  .option("partitionColumn", "id")   // 分区列
  .option("lowerBound", "0")         // 分区下界
  .option("upperBound", "1000000")   // 分区上界
  .load()
```

> JDBC 读取时，如果表很大，务必设置 `partitionColumn`、`numPartitions`、`lowerBound`、`upperBound`，这样才能分区并行读取，否则只有一个 Task 在拉数据。

### JDBC 读取原理

```
未设置分区参数时：
  一个 Task 执行 SELECT * FROM orders WHERE ...
  → 如果表有 1 亿行，单 Task 拉取全部数据
  → Driver/Executor OOM！！

设置分区参数后：
  Task 1: SELECT * FROM orders WHERE id >= 0 AND id < 100000
  Task 2: SELECT * FROM orders WHERE id >= 100000 AND id < 200000
  ...
  → 10 个 Task 各拉 1000 万行
  → 每个 Task 内存可控
```

**JDBC 写入时的性能考虑**：

```scala
// 写入 JDBC
df.write
  .mode("append")
  .option("url", "jdbc:mysql://host:3306/db")
  .option("dbtable", "orders")
  .option("batchsize", "1000")     // 批量写入大小
  .option("isolationLevel", "NONE") // 关闭事务（性能提升）
  .option("truncate", "true")      // overwrite 时用 TRUNCATE
  .format("jdbc").save()
```

> **踩坑经验**：JDBC 写入时默认的事务隔离级别是 `READ_COMMITTED`，每批次写入都自动提交事务。如果数据量大（百万级以上），建议关闭事务或增大 `batchsize`。另外，写入时要控制 `df.coalesce(n)` 的并行度，如果写 100 个分区到 MySQL，MySQL 承受不了 100 个并发的 INSERT。

## Hive 集成

Spark SQL 可以和 Hive Metastore 集成，直接读写 Hive 表。

```scala
// 启用 Hive 支持（需要 Hive 配置）
val spark = SparkSession.builder()
  .config("hive.metastore.uris", "thrift://hive-metastore:9083")
  .enableHiveSupport()
  .getOrCreate()

// 直接读取 Hive 表
val df = spark.table("default.orders")
spark.sql("SELECT count(*) FROM default.orders")

// 写入 Hive 表
df.write
  .mode("overwrite")
  .saveAsTable("default.orders")

// 创建临时表关联 Hive 表
spark.sql("CREATE TEMPORARY VIEW orders USING hive OPTIONS (table 'default.orders')")
```

### Hive 集成的关键配置

| 配置项 | 说明 | 是否必需 |
|--------|------|---------|
| `hive.metastore.uris` | Hive Metastore 的 Thrift 地址 | 是 |
| `spark.sql.warehouse.dir` | 数仓 HDFS 路径 | 否（使用 Hive 默认） |
| `spark.sql.hive.metastore.version` | Hive 版本 | 版本不匹配时需要 |

## 面试高频考点

### Q: 为什么 Parquet 比 CSV/JSON 快？

1. **列式存储**：只读需要的列，跳过无关列
2. **谓词下推**：RowGroup 级别的 min/max 统计 → 跳过不符合条件的行组
3. **压缩率高**：列式数据同类型值压缩率远高于行式
4. **Schema 内置**：不需要额外元数据

### Q: 分区和分桶的区别？

- **分区**：按列值分目录（dt=2024-01-01/），适合按时间过滤
- **分桶**：按列 Hash 分文件（桶 0~N），适合 Join/聚合

分区适用于：过滤条件明确、值有限的场景。分桶适用于：高频 Join、高频聚合的场景。

### Q: 分区列应该选什么？

1. 过滤频率高的列（最常见是日期/地域）
2. 列的基数适中（基数太大 → 小文件过多）
3. 分区后每个目录数据量均衡

### Q: 小文件问题怎么解决？

1. 写入时用 `coalesce(n)` 控制输出文件数
2. 用 `maxRecordsPerFile` 限制单文件行数
3. 定期运行合并小文件的 ETL 任务
4. 开启 AQE 的 `coalescePartitions` 动态合并

### Q: Bucket Join 的条件是什么？

两张表按相同的列、相同的桶数（或倍数关系）分桶，并且桶内的数据已排序。满足条件后 Join 时可以完全避免 Shuffle。

## 小结

| 格式 | 场景 | 推荐度 |
|------|------|--------|
| Parquet | 数据仓库、报表、ETL 结果 | ⭐⭐⭐⭐⭐ |
| ORC | Hive 存量表 | ⭐⭐⭐⭐ |
| Avro | 流式写入、Kafka 连接 | ⭐⭐⭐ |
| JSON | 日志、调试 | ⭐⭐ |
| CSV | 跨系统交换 | ⭐⭐ |

| 存储策略 | 作用 | 推荐场景 |
|---------|------|---------|
| 分区 | 过滤时跳过目录 | 时间序列、地域 |
| 分桶 | Join 时消除 Shuffle | 高频 Join 的表 |
| Parquet 压缩 | 减少存储和 IO | 所有生产数据 |
