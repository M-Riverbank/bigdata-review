# Spark SQL — 性能优化与高频考点

## 为什么需要性能优化

Spark SQL 虽然拥有 Catalyst 优化器和 WholeStageCodegen 等自动化技术，但它不是万能的。在复杂的大数据场景下，**数据分布、SQL 写法、资源配置**都会显著影响最终性能。

一个常见的现象：同一个查询，在 10GB 数据上跑 5 分钟，在 100GB 上跑 2 小时——性能不是线性增长的，往往是指数级恶化的。原因可能只是某个 Join 触发了数据倾斜、或者 Shuffle 分区数不合适。

> **面试点**：Spark SQL 调优面试题的核心逻辑：**先看数据，再看代码，最后调参数**。不要一上来就调参数，90% 的性能问题可以通过优化 SQL 写法或数据组织方式解决。

### 性能优化的三个层面

```
1. 代码优化 — SQL 写法、API 选择
   └── 见效最快，改动最小
   
2. 配置优化 — AQE、Shuffle、Broadcast
   └── 一次配置全局生效

3. 架构优化 — 分区、分桶、文件格式
   └── 效果最持久，但需要在设计阶段考虑
```

## SQL 写法优化

### 尽早过滤

这是最重要的 SQL 优化原则——**过滤条件尽可能早地应用**。

```sql
-- ❌ 低效：先 Join 再过滤
SELECT * FROM (
  SELECT a.id, a.amount, b.name
  FROM orders a JOIN users b ON a.user_id = b.id
) WHERE a.dt = '2024-01-01'

-- ✅ 高效：先过滤再 Join
SELECT a.id, a.amount, b.name
FROM (SELECT * FROM orders WHERE dt = '2024-01-01') a
JOIN users b ON a.user_id = b.id
```

为什么先过滤再 Join 更快？

```
低效版本执行计划：
  1. 读 orders 全表（1 亿行）
  2. 读 users 全表（1000 万行）
  3. 两表 Join（1 亿 × 1000 万）
  4. 再 WHERE 过滤（只剩 30 万行）

高效版本执行计划：
  1. 读 orders 中 dt 条件（30 万行）
  2. 读 users 全表（1000 万行）
  3. 两表 Join（30 万 × 1000 万）
  4. 无需再过滤
```

### 避免 SELECT *

```sql
-- ❌ 低效
SELECT * FROM orders

-- ✅ 高效：只读需要的列
SELECT order_id, user_id, amount FROM orders
```

对行式存储（CSV/JSON）来说，`SELECT *` 和指定列的性能差异巨大（所有列都要读）。对列式存储（Parquet/ORC）来说，差异较小但依然存在（列裁剪虽然能跳过不读，但执行计划中仍然需要解析所有列的信息）。

### 使用列式存储

```sql
-- Parquet/ORC 默认列裁剪（只读需要的列）
-- CSV/JSON 必须读完整行再裁剪
-- 所以：Parquet 中 SELECT * 影响不大，但 CSV 中影响大
```

### 用 withColumn 替代多次 select

```scala
// ❌ 低效：多次 select（每次新建 LogicalPlan）
df.select($"user_id", $"amount")
  .select($"user_id", ($"amount" * 1.1).alias("tax"))

// ✅ 高效：一次 withColumn
df.withColumn("tax", $"amount" * 1.1)
```

但实际上 Catalyst 的投影合并规则会优化掉这些多余的 select。这个点更多是代码可读性层面的优化，性能差异不大。

### 善用谓词下推

```sql
-- ❌ 低效：过滤条件在子查询外部
SELECT * FROM (
  SELECT * FROM orders
) WHERE dt = '2024-01-01'

-- ✅ 高效：过滤条件直接作用于表
SELECT * FROM orders WHERE dt = '2024-01-01'
```

虽然 Catalyst 的谓词下推规则会自动将 `dt = '2024-01-01'` 推进子查询，但不是所有场景都能成功下推。特别是 UDF 和复杂子查询场景，手动写在内层更可靠。

## Join 优化

### 小表广播

```scala
// 自动广播（默认 10MB）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "52428800")  // 50MB

// 显式广播
import org.apache.spark.sql.functions.broadcast
largeDF.join(broadcast(smallDF), "key")

// 查看 Join 策略
largeDF.join(smallDF, "key").explain()
// == Physical Plan ==
// BroadcastHashJoin [key], BuildRight, ...
```

### Bucket Join

```scala
// 两张表都按 userId 分桶
df.write.bucketBy(128, "user_id").saveAsTable("orders_bucketed")
dim.write.bucketBy(128, "user_id").saveAsTable("users_bucketed")

// Join 时无需 Shuffle（数据已经按 key 分布在对应桶中）
spark.table("orders_bucketed")
  .join(spark.table("users_bucketed"), "user_id")
  .explain()
// 不会有 Exchange hashpartitioning 节点！
```

### Join 顺序优化

```
小表 JOIN 大表 → 小表作为 Build Side（Broadcast）
中等表 JOIN 大表 → 确保排序键一致（避免额外排序）
大表 JOIN 大表 → 考虑分桶预关联
```

### Join 策略选择指南

| 场景 | 推荐策略 | 说明 |
|------|---------|------|
| 大表 Join 小表 (< 10MB) | Broadcast Hash Join | 自动生效 |
| 大表 Join 中表 (< 100MB) | 显式 broadcast hint | 手动标记 |
| 大表 Join 大表 | Sort Merge Join | 默认策略，稳定 |
| 两张分桶表 Join | Bucket Join | 从源头消除 Shuffle |
| 非等值 Join | Nested Loop Join | 尽量避免 |

## 数据倾斜处理

### 倾斜诊断

```sql
-- 查找倾斜 Key
SELECT key, COUNT(*) as cnt
FROM table
GROUP BY key
ORDER BY cnt DESC
LIMIT 10

-- 在 Spark UI 中：
-- Stage 页 → 看 Task 耗时是否严重不均
-- SQL 页 → 看 Shuffle 各分区数据量
```

诊断数据倾斜的三种方式：

| 方式 | 操作 | 判断依据 |
|------|------|---------|
| SQL 查询 | `GROUP BY key ORDER BY cnt DESC` | 某个 key 的行数远超平均值 |
| Spark UI Stage 页 | 查看 Task 耗时分布 | 少数 Task 耗时远高于平均 |
| Spark UI SQL 页 | 查看 Shuffle 各分区大小 | 某分区数据量远超其他分区 |

### 倾斜解决方案

**方案一：加盐（两阶段聚合）**

```scala
// 适用于 GroupBy 场景的数据倾斜
val skewedDF = df.withColumn(
  "salt_key",
  when($"key" === "skewed_value", concat($"key", lit("_"), (rand() * 100).cast("int")))
  .otherwise($"key")
)
skewedDF.groupBy("salt_key").agg(sum("amount"))
  .groupBy(substring($"salt_key", 1, length($"salt_key") - 3))
  .agg(sum("sum(amount)"))
```

加盐的原理：

```
第一阶段：给倾斜 key 加随机后缀（1-100），打散到一个 key 变成 100 个 key
  → 原本一个 Task 处理全部倾斜数据 → 变成 100 个 Task 各处理 1%

第二阶段：去掉随机后缀，对初步聚合结果做二次聚合
  → 将 100 个随机 key 的结果合并为 1 个 key 的结果
```

**方案二：广播 + 过滤**

```scala
// 适用于 Join 场景的数据倾斜
val skewedKeys = Seq("skewed_value")
val normalPart = df.filter(!$"key".isin(skewedKeys: _*))
val skewedPart = df.filter($"key".isin(skewedKeys: _*))

// 对倾斜部分：广播另一侧 + map Join
val result = normalPart.join(smallTable, "key")
  .union(skewedPart.join(broadcast(smallTable), "key"))
```

**方案三：AQE 自动处理（Spark 3.0+ 推荐）**

```scala
// 开启 AQE，自动处理 Join 倾斜
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

| 方案 | 适用场景 | 复杂性 | 推荐度 |
|------|---------|--------|--------|
| AQE 自动处理 | Join 倾斜 | 零改动 | ⭐⭐⭐⭐⭐ |
| 加盐 | GroupBy 倾斜 | 中等 | ⭐⭐⭐⭐ |
| 广播+过滤 | Join 倾斜（已知倾斜 key） | 中等 | ⭐⭐⭐⭐ |
| 增大分区数 | 轻微倾斜 | 低 | ⭐⭐⭐ |

## 文件与存储优化

### 小文件问题

小文件是 Spark 写入后最常见的问题。每个小文件都要占用 HDFS NameNode 的内存。

```scala
// 问题：大量小文件（Spark 写入产生）

// 方案 1：写入时控制文件大小
df.coalesce(10).write.parquet("hdfs://output")

// 方案 2：读取后重新分区
spark.read.parquet("hdfs://input").repartition(100)

// 方案 3：AQE 自动合并
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

小文件问题的判断标准：

| 指标 | 健康 | 警告 | 危险 |
|------|------|------|------|
| 单文件大小 | 128MB-1GB | < 64MB | < 16MB |
| 文件总数（1TB 数据） | < 8000 | 8000-16000 | > 16000 |
| NameNode 内存占用 | 正常 | 偏高 | 危险 |

### 合理分区

```sql
-- 分区建议：
-- 1. 分区列应该经常出现在 WHERE 中
-- 2. 每个分区数据量 100MB~1GB
-- 3. 分区列基数不要太大（> 10000 会小文件过多）

-- 例如：按天分区
CREATE TABLE orders (
  order_id BIGINT,
  user_id BIGINT,
  amount DOUBLE
) PARTITIONED BY (dt STRING)
STORED AS PARQUET;
```

## 配置优化

```scala
// 必开配置
spark.conf.set("spark.sql.adaptive.enabled", "true")  // AQE
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

// Shuffle 配置
spark.conf.set("spark.sql.shuffle.partitions", "200")  // 默认，根据数据量调整
// 经验公式：每个分区 100~200MB

// Broadcast 配置
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")  // 默认 10MB

// 文件格式配置
spark.conf.set("spark.sql.parquet.compression.codec", "snappy")
spark.conf.set("spark.sql.parquet.mergeSchema", "false")  // 关掉减少开销
```

### 关键配置参数汇总

| 配置项 | 默认值 | 建议值 | 说明 |
|--------|--------|--------|------|
| `adaptive.enabled` | true (3.2+) | true | AQE 总开关 |
| `adaptive.skewJoin.enabled` | true | true | 倾斜检测 |
| `adaptive.coalescePartitions.enabled` | true | true | 动态合并 |
| `adaptive.advisoryPartitionSizeInBytes` | 64MB | 64-256MB | 目标分区大小 |
| `shuffle.partitions` | 200 | 200-1000 | Shuffle 分区数 |
| `autoBroadcastJoinThreshold` | 10MB | 10-100MB | 广播阈值 |
| `parquet.compression.codec` | snappy | snappy/zstd | 压缩算法 |

## 性能排查方法论

### 慢查询排查流程

```
1. ❓任务很慢？
   │
   ├─ 看 Spark UI
   │     ├─ 有 Task 特别慢 → 数据倾斜（查倾斜 key）
   │     └─ 所有 Task 都慢 → 资源不足
   │
   ├─ 看执行计划
   │     ├─ 有 Exchange 但数据量不大 → Broadcast 效果更好
   │     └─ Filter 在 Scan 之后很远 → 谓词没下推
   │
   ├─ 看数据量
   │     ├─ 数据量远大于预期 → 检查 WHERE 条件
   │     └─ 小文件过多 → 需要合并
   │
   └─ 看资源配置
         ├─ Executor 内存太小 → 频繁 GC/溢写
         └─ 并行度不足 → CPU 利用率低
```

### Q: 一个 Spark SQL 任务跑得很慢，怎么排查？

1. **看 Spark UI** → 哪个 Stage 最慢 → 展开看 Task 分布
2. **如果有 Task 特别慢** → 数据倾斜 → 查倾斜 key
3. **Task 都一样慢** → 资源不足 → 看是否 CPU/内存/IO 瓶颈
4. **检查执行计划** → `df.explain()` → 看有没有不必要的 Shuffle
5. **检查数据量** → 数据量是否预估正确

### Q: 如何判断需要多少 shuffle.partitions？

根据 Shuffle 后的数据量估算：假设数据量 200GB，目标每个分区 200MB → 需要 200GB / 200MB ≈ 1000 个分区。也可以通过 AQE 动态合并。

### Q: Spark SQL 和 Hive 有什么本质区别？

- **执行引擎**：Spark DAG vs Hive MR/Tez
- **中间结果**：Spark 内存 + 磁盘 vs Hive 磁盘
- **优化器**：Catalyst vs Hive optimizer（Catalyst 更强）
- **代码生成**：WholeStageCodegen vs 无

## 面试高频考点

### Q: 写过哪些 Spark SQL 优化？效果如何？

这是开放性面试题，建议从以下角度回答：

| 优化手段 | 典型效果 | 难易程度 |
|---------|---------|---------|
| 开启 AQE | 大查询提速 20-50% | 简单（一行配置） |
| Broadcast 小表 | 消除 Shuffle，提速 2-10x | 简单 |
| 数据倾斜处理 | 避免单 Task 超时，效果显著 | 中等 |
| Parquet 替代 CSV | IO 减少 70-90% | 简单 |
| 分区裁剪 | 查询时间与扫描分区数成正比 | 简单 |
| 分桶 Join | 消除 Shuffle，提速 2-5x | 中等 |

### Q: 大表 Join 大表时怎么做性能优化？

1. 开启 AQE（运行时自动优化）
2. 如果两张表 Join 频繁，考虑按 Join key 分桶
3. 先过滤非必要数据，减少 Join 数据量
4. 如果业务允许，用宽表反范式化避免 Join

### Q: 数据倾斜怎么处理？

先判断是 Join 场景还是 GroupBy 场景。Join 场景用 AQE 或"广播+过滤"方案。GroupBy 场景用"加盐"方案。优先尝试 AQE，零代码改动。

### Q: 怎么判断一个查询有没有触发 Broadcast Join？

用 `df.explain()` 查看执行计划。如果看到 `BroadcastHashJoin` 或 `BroadcastExchange`，说明使用了广播。如果看到 `SortMergeJoin` 或 `Exchange hashpartitioning`，说明没有广播。

## 小结

| 优化类型 | 核心手段 | 效果 |
|---------|---------|------|
| SQL 写法 | 尽早过滤、避免 SELECT * | 减少 IO |
| Join | Broadcast Join、Bucket Join | 消除 Shuffle |
| AQE | 动态合并、动态 Join、倾斜处理 | 运行时自适应 |
| 文件 | Parquet/分区/分桶 | IO + 压缩率 |
| 配置 | shuffle.partitions 调优 | 负载均衡 |
| 数据倾斜 | AQE/加盐/广播+过滤 | 避免单点瓶颈 |
