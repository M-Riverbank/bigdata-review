# Spark SQL Join 策略与 Catalyst 优化器

## 为什么需要了解 Join 策略

Join 是大数据处理中最常见也最昂贵的操作之一。在关系型数据库中，一个 Join 可能只需要几毫秒；但在分布式系统中，一个 Join 可能涉及：

- **数据重分区**：几十 TB 的数据通过网络 Shuffle
- **排序**：几百亿行数据在内存和磁盘间排序
- **内存压力**：构建 Hash 表的 Executor 可能 OOM

理解 Join 策略的本质是理解：**数据在分布式环境下是怎么合并的？** 不同的策略对应不同的 Shuffle 开销、内存开销和 CPU 开销。

> **面试点**：面试官常问"Spark SQL 有哪些 Join 策略，分别适合什么场景？"这是考察对分布式 Join 底层原理的理解。回答时需要讲清楚每种策略的执行过程（Shuffle 与否、内存使用、排序需求），而不是只背名字。

## Join 策略总览

Spark SQL 支持 5 种 Join 策略，优化器按成本模型自动选择：

| 策略 | 触发条件 | Shuffle | 适用场景 |
|------|---------|---------|---------|
| Broadcast Hash Join (BHJ) | 一端 < 阈值 | 无 | 大表 JOIN 小表 |
| Shuffle Hash Join (SHJ) | 一端 < 内存阈值 | 有 | 中等表 JOIN 中等表 |
| Sort Merge Join (SMJ) | 默认（两端都大） | 有 | 大表 JOIN 大表 |
| Broadcast Nested Loop Join (BNLJ) | 笛卡尔积 | 无 | 极特殊情况 |
| Shuffle-and-Replicate NLJ (CAJ) | 无等值条件 | 有 | 笛卡尔积 |

> **面试点**：Spark 3.4 之后，Join 策略选择的优先级：BHJ > SHJ > SMJ。如果小表足够小，优先广播；否则看是否满足 Hash Join 条件；最后兜底用 Sort Merge Join。

## 1. Broadcast Hash Join（最优策略）

Broadcast Hash Join 是所有 Join 策略中**性能最优**的，因为它完全避免了 Shuffle。

```
小表 < broadcast 阈值（默认 10MB），广播到所有 Executor

┌───────────────────┐
│ 小表（广播）        │
│ [1,a] → 全部节点   │
└──┬────────┬───────┘
   │        │
   ▼        ▼
┌──────┐ ┌──────┐
│Exec A│ │Exec B│  ← 大表数据本地读取
│大表  │ │大表  │    无 Shuffle！
│Hash  │ │Hash  │
│Join  │ │Join  │
└──────┘ └──────┘
```

```scala
// 显式使用
import org.apache.spark.sql.functions.broadcast
largeDF.join(broadcast(smallDF), "key")

// 查看执行计划
df.explain("extended")
// == Optimized Logical Plan ==
// Join ... using BroadcastHashJoin

// 调整阈值（生产环境可调大，但注意内存）
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 104857600L)  // 100MB
```

### 条件

- 一端大小 < `spark.sql.autoBroadcastJoinThreshold`（默认 10MB = 10485760 字节）
- 广播端放入每个 Executor 内存（不是 Driver！）

### 执行流程

```
1. Driver 读取小表的数据，估算大小
2. 如果小表 < autoBroadcastJoinThreshold，打上 BroadcastHashJoin 标记
3. 小表通过 BitTorrent 协议分发到所有 Executor
4. 每个 Executor 用小表构建本地 Hash 表
5. 大表数据在本地与 Hash 表做 Join（无网络传输）
```

> **踩坑经验**：`autoBroadcastJoinThreshold` 默认 10MB，这个值对现代集群来说偏小。在内存充足的集群上（每个 Executor 8GB+），可以调到 50-100MB。但要注意：如果一张表 50MB，集群有 100 个 Executor，那么广播这张表总共需要 50MB × 100 = 5GB 的网络传输和内存占用。**广播不是免费的，表太大依然会导致 Executor OOM。**

### 判断是否启用了广播

```scala
// 方式1：查看执行计划
df.explain()
// 如果看到 BroadcastHashJoin 或 BroadcastExchange → 已启用广播

// 方式2：查看物理计划详情
df.queryExecution.executedPlan
// BroadcastHashJoin [key], BuildRight, ...
// :- BroadcastExchange HashedRelationBroadcastMode ...
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.sql.autoBroadcastJoinThreshold` | 10485760 (10MB) | 广播阈值，-1 禁用广播 |
| `spark.sql.broadcastTimeout` | 300s | 广播超时时间 |
| `spark.sql.adaptive.autoBroadcastJoinThreshold` | (none) | AQE 模式下广播阈值 |

> **面试点**：如果小表刚好 12MB（超过阈值），还可以用广播吗？可以，两种方式：（1）手动调高 `autoBroadcastJoinThreshold`；（2）代码中显式用 `broadcast()` hint。手动 hint 会忽略阈值限制。

## 2. Sort Merge Join（默认大表 Join）

当两表都很大且不满足广播条件时，Spark 默认使用 Sort Merge Join。它的核心思想是：**先排序，后归并**。

```
Step 1: 两端按 join key 重分区 (Shuffle)
Step 2: 每个分区内按 join key 排序
Step 3: 双指针归并 Join

大表 A (userId):        大表 B (userId):
[1, a1]  ┐              [1, b1]  ┐
[2, a2]  ├─ Shuffle →   [2, b2]  ├─ Shuffle →
[1, a3]  ┘              [1, b3]  ┘

Partition 1:             Partition 1:
[1, a1] ←──归并──→      [1, b1]
[1, a3] ←──归并──→      [1, b3]

Partition 2:             Partition 2:
[2, a2] ←──归并──→      [2, b2]
```

```scala
// 默认策略，无需显式指定
// 但在数据倾斜时可能很慢
```

### 执行流程

```
1. Shuffle：两张表按 join key 哈希到相同的分区
2. Sort：每个分区内按 join key 排序
3. Merge：双指针遍历两个已排序的分区，key 相等则输出
```

### 优化技巧

```scala
// 1. 通过分桶避免 Shuffle — Bucket Join
dfA.write.bucketBy(128, "key").sortBy("key").saveAsTable("bucketed_a")
dfB.write.bucketBy(128, "key").sortBy("key").saveAsTable("bucketed_b")
spark.sql("SELECT * FROM bucketed_a JOIN bucketed_b USING(key)")
// → 相同 key 在同一分区，无需 Shuffle！

// 2. 自适应执行 (AQE)
spark.conf.set("spark.sql.adaptive.enabled", true)
// AQE 自动优化：合并小分区、将 SMJ 转为 BHJ（如果 Shuffle 后数据变小）
```

> **面试点**：Bucket Join 是面试中的高级话题。它的原理是：两张表在建表时按相同的方式分桶（相同桶数、相同分桶列），写入时相同 key 的数据就落到了同一个分区。Join 时，相同分区的数据在本地直接做 Merge Join，完全不需要 Shuffle。这是**从源头消除 Shuffle**的最根本方法。

## 3. Shuffle Hash Join

Shuffle Hash Join 是 Broadcast Hash Join 和 Sort Merge Join 的折中方案。

```
流程：
1. Shuffle：两表按 join key 重分区
2. Build：Spark 选择较小的一端构建 Hash 表
3. Probe：另一端探测 Hash 表做 Join
```

```scala
// 需要显式开启（默认关闭）
spark.conf.set("spark.sql.join.preferSortMergeJoin", false)
```

> 与 SMJ 的区别：SMJ 用排序 + 归并，SHJ 用 Hash 表。SHJ 对一份数据构建 Hash 表，另一份探测，通常更快但不是所有场景都适用。

### SHJ 的触发条件

- 一端能放入内存构建 Hash 表
- 但两端都大于广播阈值（无法使用 BHJ）

### SHJ vs SMJ 对比

| 维度 | SHJ | SMJ |
|------|-----|-----|
| 构建阶段 | 构建 Hash 表 | 排序 |
| 内存需求 | 高（需要全量 Hash 表） | 低（排序可溢写磁盘） |
| 等值 Join | 支持 | 支持 |
| 非等值 Join | 不支持 | 支持（排序后范围过滤） |
| 数据倾斜影响 | 大（倾斜的 key 导致 Hash 冲突） | 大（倾斜分区排序慢） |

## 4. 其他 Join 策略

### Broadcast Nested Loop Join (BNLJ)

当 Join 条件不是等值条件时（如 `a.value < b.value`），无法使用 Hash Join，只能用 Nested Loop Join。BNLJ 将一端广播到每个 Executor，然后在另一端逐行遍历。

```scala
// 非等值 Join 会触发 BNLJ
largeDF.join(smallDF, largeDF("value") > smallDF("value"))
```

### Shuffle-and-Replicate NLJ

当两表都很大且无等值条件时，Shuffle 后每节点都包含两端数据，做 Nested Loop Join。**应尽量避免，性能极差**。

## 实际 Join 策略选择指南

| 场景 | 推荐策略 | Shuffle | 原因 |
|------|---------|---------|------|
| 大表 Join 小表 (<10MB) | BHJ | 无 | 首选，无 Shuffle |
| 大表 Join 中表 (<100MB) | BHJ (手动广播) | 无 | 调阈值或显式广播 |
| 中表 Join 中表 | SHJ (显式开启) | 有 | 比 SMJ 少一次排序 |
| 大表 Join 大表 | SMJ | 有 | 稳定可靠，可溢写磁盘 |
| 同一分桶键 Join | Bucket Join | 无 | 从源头消除 Shuffle |
| 非等值 Join | BNLJ/NLJ | 有/无 | 尽量避免 |

## Catalyst 优化器核心流程

Catalyst 是 Spark SQL 的查询优化引擎，它负责将用户写的 SQL/DataFrame API 翻译成高效的物理执行计划。

```
┌─────────────────────────────────────────────────────────────────┐
│                        Catalyst 优化器                           │
│                                                                 │
│  SQL String                                                     │
│      │                                                          │
│  ┌───▼─────────────────────────────────────┐                   │
│  │ 1. Parser (ANTLR4)                       │                   │
│  │    生成 Unresolved Logical Plan           │                   │
│  │    - 表名/列名尚未解析                     │                   │
│  └───┬─────────────────────────────────────┘                   │
│      │                                                          │
│  ┌───▼─────────────────────────────────────┐                   │
│  │ 2. Analyzer (Catalog + Rule)             │                   │
│  │    - 解析表名 → 从 Catalog 获取 Schema     │                   │
│  │    - 解析列名 → 检查列是否存在              │                   │
│  │    - 类型推断 → 自动推导结果类型             │                   │
│  └───┬─────────────────────────────────────┘                   │
│      │                                                          │
│  ┌───▼─────────────────────────────────────┐                   │
│  │ 3. Optimizer (Rule-based + Cost-based)   │                   │
│  │    - 谓词下推 (PushDownPredicate)         │                   │
│  │    - 列裁剪 (ColumnPruning)               │                   │
│  │    - 常量折叠 (ConstantFolding)           │                   │
│  │    - Join 重排序 (ReorderJoin)            │                   │
│  │    - 投影合并 (CollapseProject)           │                   │
│  └───┬─────────────────────────────────────┘                   │
│      │                                                          │
│  ┌───▼─────────────────────────────────────┐                   │
│  │ 4. Planner (SparkPlanner)                │                   │
│  │    生成多个 Physical Plan，成本模型选最优    │                   │
│  │    - 选 Join 策略 (BHJ > SMJ > SHJ)      │                   │
│  │    - 选排序策略                           │                   │
│  └───┬─────────────────────────────────────┘                   │
│      │                                                          │
│  ┌───▼─────────────────────────────────────┐                   │
│  │ 5. Code Generation (WholeStageCodegen)   │                   │
│  │    将多个算子编译为一个 Java 函数            │                   │
│  │    - 减少虚函数调用                        │                   │
│  │    - 手写 while 循环                       │                   │
│  └─────────────────────────────────────────┘                   │
└─────────────────────────────────────────────────────────────────┘
```

### 查看执行计划

```scala
// Parsed Logical Plan（解析后的逻辑计划）
df.explain("simple")

// Analyzed Logical Plan + Optimized Logical Plan + Physical Plan
df.explain("extended")

// 所有计划 + 成本
df.explain("codegen")  // 含代码生成

// 另一种方式
df.queryExecution.logical       // 逻辑计划（优化前）
df.queryExecution.optimizedPlan // 逻辑计划（优化后）
df.queryExecution.executedPlan  // 物理计划
```

## 关键优化规则详解

### 谓词下推（PushDownPredicate）

谓词下推是 Catalyst 优化器最重要的优化之一。它把 `WHERE` 条件尽可能早地应用到数据源上，减少 IO 和计算量。

```sql
-- 原始
SELECT * FROM orders WHERE amount > 100 AND dt = '2024-01-01'

-- 优化后
-- 1. 分区过滤器 dt='2024-01-01' → 跳过不匹配的分区（目录级别）
-- 2. amount > 100 → 推到 Parquet RowGroup 级别（min/max 统计过滤）
-- Parquet Reader 只读符合条件的 RowGroup
```

### 列裁剪（ColumnPruning）

列裁剪只读取 SQL 中引用的列，对列式存储文件（Parquet/ORC）效果尤其显著。

```sql
-- 表有 100 列，只读取 3 列
SELECT name, age FROM users;

-- 列式存储 (Parquet/ORC) → 只读 3 列的 stripe
-- 100 列 → 3 列，IO 减少 97%
```

### Join 重排序（ReorderJoin）

```sql
-- 原始: A JOIN B JOIN C
-- 假设 A=10GB, B=1MB, C=100GB
-- 优化后: A JOIN C → 作为小表的 B 被先放到 Broadcast (与 B 的 Join 被重排到合适位置)
```

## 自适应查询执行 (AQE) — Spark 3.0+

AQE（Adaptive Query Execution）在运行时收集统计信息，动态调整执行计划，是 Spark 3.0 最重要的性能特性。

```scala
spark.conf.set("spark.sql.adaptive.enabled", true)

// 功能1: 动态合并小分区
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true)
// 初始 400 分区 → Shuffle 后每分区数据很少 → 自动合并为 80 分区

// 功能2: 动态切换 Join 策略
spark.conf.set("spark.sql.adaptive.localShuffleReader.enabled", true)
// SMJ 的 Shuffle 数据落地后，发现一端 < broadcast 阈值 → 自动转 BHJ

// 功能3: 动态处理数据倾斜
spark.conf.set("spark.sql.adaptive.skewedJoin.enabled", true)
// 发现某个 key 数据量远超平均 → 自动拆分倾斜分区
```

> **面试点**：AQE 的核心价值在于"运行时自适应"。以前的优化器只能基于静态的统计信息做决策（比如表的行数可能是 3 个月前的数据），但 AQE 在每次 Shuffle 后都能拿到最准确的数据大小信息，动态调整后续的执行计划。比如 Shuffle 后发现某分区特别大（数据倾斜），自动拆分成多个小分区并行处理。

## 面试高频考点

### Q: 大表 Join 大表时怎么做性能优化？

1. 开启 AQE（动态处理数据倾斜）
2. 如果 Join 键是分桶键，用 Bucket Join 消除 Shuffle
3. 提前过滤：先 WHERE 再 Join，减少 Join 数据量
4. 如果数据允许，考虑宽表（反范式化，避免 Join）

### Q: Broadcast Hash Join 为什么快？

因为它消除了 Shuffle。小表广播到所有节点后，每个节点本地做 Map 端 Join，不需要跨网络传输数据。

### Q: Sort Merge Join 和 Shuffle Hash Join 选哪个？

SMJ 更稳定（排序可溢写磁盘），SHJ 更快（Hash 表探测）。SHJ 需要一端能完全放入内存。一般推荐 SMJ（默认），在内存充足的场景下可以开启 SHJ。

## 小结

| 主题 | 核心建议 |
|------|---------|
| BHJ | 首选策略，数据倾斜的克星（大表 Join 小表变 Map Join） |
| SMJ | 两个大表的默认策略，稳定可控 |
| AQE | Spark 3.0+ 必开！动态优化 + 倾斜处理 |
| Catalyst | 理解优化器 = 写出高分面试答案 |
| Bucket Join | 从源头消除 Shuffle 的根本方法 |
