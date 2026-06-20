# Spark SQL Join 策略与 Catalyst 优化器

## Join 策略总览

Spark SQL 支持 5 种 Join 策略，优化器按成本模型自动选择：

| 策略 | 触发条件 | Shuffle | 适用场景 |
|------|---------|---------|---------|
| Broadcast Hash Join (BHJ) | 一端 < 阈值 | 无 | 大表 JOIN 小表 |
| Shuffle Hash Join (SHJ) | 一端 < 内存阈值 | 有 | 中等表 JOIN 中等表 |
| Sort Merge Join (SMJ) | 默认（两端都大） | 有 | 大表 JOIN 大表 |
| Broadcast Nested Loop Join (BNLJ) | 笛卡尔积 | 无 | 极特殊情况 |
| Shuffle-and-Replicate NLJ (CAJ) | 无等值条件 | 有 | 笛卡尔积 |

## 1. Broadcast Hash Join（最优策略）

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

## 2. Sort Merge Join（默认大表 Join）

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

## 3. Shuffle Hash Join

两端 Shuffle 后，在各自分区内构建 Hash 表做 Join：

```scala
// 需要显式开启（默认关闭）
spark.conf.set("spark.sql.join.preferSortMergeJoin", false)
```

> 与 SMJ 的区别：SMJ 用排序 + 归并，SHJ 用 Hash 表。SHJ 对一份数据构建 Hash 表，另一份探测，通常更快但不是所有场景都适用。

## Catalyst 优化器核心流程

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

```sql
-- 原始
SELECT * FROM orders WHERE amount > 100 AND dt = '2024-01-01'

-- 优化后
-- 1. 分区过滤器 dt='2024-01-01' → 跳过不匹配的分区（目录级别）
-- 2. amount > 100 → 推到 Parquet RowGroup 级别（min/max 统计过滤）
-- Parquet Reader 只读符合条件的 RowGroup
```

### 列裁剪（ColumnPruning）

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

## 面试高频考点

### Q: Spark SQL 的 Join 策略选择？

```
1. 有一端 < 10MB → Broadcast Hash Join
2. 两端都 > 10MB：
   a. 默认 → Sort Merge Join
   b. 强制 key 可 hash + 一端能放入内存 → Shuffle Hash Join
3. 笛卡尔积（无等于条件）→ BroadcastNestedLoopJoin
```

### Q: 为什么 Sort Merge Join 不需要把数据全放入内存？

两端分别排序后，归并操作只需缓存当前 key 的所有行。同 key 的行处理完即可释放。内存需求是 O(单 key 的最大行数)。

### Q: 什么 Join 会触发全表 Shuffle？

等值 Join（a.id = b.id）的两个大表，且没有按 id 预分桶。

**避免全量 Shuffle 的方法**：
1. 小表 → Broadcast Join
2. 预写分桶表 → Bucket Join
3. 使用 AQE 自适应

### Q: Catalyst 优化器做了哪些具体优化？

| 规则 | 效果 |
|------|------|
| PushDownPredicate | Where 条件推到数据源 |
| ColumnPruning | 只读需要的列 |
| ConstantFolding | 编译期计算常量 |
| ReorderJoin | 按表大小重排 Join 顺序 |
| CollapseProject | 合并连续的 select |
| NullPropagation | 简化含 null 的表达式 |
| SimplifyCasts | 消除不必要的类型转换 |

## 小结

| 主题 | 核心建议 |
|------|---------|
| BHJ | 首选策略，数据倾斜的克星（大表 Join 小表变 Map Join） |
| SMJ | 两个大表的默认策略，稳定可控 |
| AQE | Spark 3.0+ 必开！动态优化 + 倾斜处理 |
| Catalyst | 理解优化器 = 写出高分面试答案 |
| Bucket Join | 从源头消除 Shuffle 的根本方法 |
