# Spark SQL — Catalyst 优化器

## 为什么需要 Catalyst 优化器

在 Spark SQL 出现之前，Spark 执行 SQL 查询的方式非常"笨"——直接将 SQL 翻译成 RDD 算子，没有全局优化。这意味着：

- 你写 `SELECT * FROM orders WHERE amount > 100`，Spark 会老老实实地把全表读出来再过滤
- 你写 `SELECT name FROM users`（表有 100 列），Spark 会把 100 列全读出来
- 连续的两个 `SELECT` 会产生两个独立的计划节点，哪怕它们可以被合并

这些"不聪明"的行为导致了大量不必要的 IO 和计算。

Catalyst 优化器正是为了解决这些问题而生的。它不是简单地把 SQL 翻译成执行计划，而是**对执行计划做等价变换**，找到执行代价最小的方案。

> **面试点**：Catalyst 优化器最核心的理念是"基于规则的优化"（RBO）。它预定义了 60+ 条优化规则，每条规则都是一个"模式匹配"——匹配到特定模式就做等价变换。注意，它是基于规则的，不是基于成本的（Spark 3.0+ 才引入了部分 CBO 特性）。

### 一个简单的优化例子

```
原始 SQL：SELECT name FROM (SELECT * FROM users) WHERE age > 18

没有优化时：
  1. 读 users 全表（id, name, age, city, phone...）
  2. 输出所有列给子查询
  3. 子查询选择 name 列
  4. 过滤 age > 18

Catalyst 优化后：
  1. 列裁剪：只读 name 和 age 两列
  2. 谓词下推：先过滤 age > 18，再 select name
  3. 投影合并：省略不必要的子查询

最终效果：IO 从 8 列变成 2 列，行数从全表变成部分
```

## Catalyst 架构

Catalyst 是 Spark SQL 的查询优化器，基于**函数式树结构** + **规则匹配**实现。它是 Spark SQL 比 Hive 快 10-100 倍的核心原因。

```
用户 API（SQL / DataFrame）
    │
    ▼
┌─────────────────────────────────────┐
│         1. ANALYSIS                  │
│  Unresolved Logical Plan → Resolved  │
│  - 连接 Catalog 校验表和列名          │
│  - 确定字段类型                      │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│         2. LOGICAL OPTIMIZATION      │
│  Resolved Logical Plan → Optimized   │
│  - 谓词下推、列裁剪、常量折叠         │
│  - Join 重排序、投影合并             │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│         3. PHYSICAL PLANNING         │
│  Optimized Plan → Physical Plans     │
│  - 生成多个候选物理计划               │
│  - 成本模型选择最优                  │
└──────────────┬──────────────────────┘
               │
┌──────────────▼──────────────────────┐
│      4. CODE GENERATION             │
│  WholeStageCodegen                  │
│  - 将算子编译为 Java 字节码          │
│  - 消除虚函数调用                    │
└─────────────────────────────────────┘
```

### Catalyst 的核心抽象

Catalyst 基于两个核心抽象：

| 抽象 | 说明 | 举例 |
|------|------|------|
| **TreeNode** | 所有计划节点都是树节点 | `Project`、`Filter`、`Join` 都是 TreeNode |
| **Rule** | 树到树的等价变换 | `PushDownPredicate`、`ColumnPruning` |

```
所有 Plan 都是 TreeNode
         Project(name, age)
              │
          Filter(age > 18)
              │
         Relation(users)

Rule 匹配特定模式并转换：
  Filter(predicate) → 匹配到 → 尝试下推到数据源
```

## 阶段一：Analysis（语义解析）

Analysis 阶段的核心工作是**验证语句的语义正确性**。SQL 字符串只是文本，Spark 需要知道表名是否存在、列名是否正确、数据类型是否匹配。

```
SQL: SELECT name, age FROM users WHERE age > 18

1. Parser: 使用 ANTLR 将 SQL 解析为 AST（抽象语法树）
   → Unresolved Logical Plan
   → 表名 "users" 未验证
   → 列名 "name", "age" 未验证

2. Analyzer: 连接 Catalog 检查
   → Catalog 中查到 users 表定义（id, name, age, city）
   → 确定 name: StringType, age: IntegerType
   → Resolved Logical Plan ✓
```

```scala
// 查看未解析的逻辑计划
df.queryExecution.analyzed
// 如果列名不存在，在这里就会报错
```

### Catalog 在 Analysis 中的作用

Catalog 是 Spark SQL 的元数据中心。它知道：

| 元数据 | 含义 |
|--------|------|
| 表名 → Schema | `orders` 表有哪些列，各是什么类型 |
| 表名 → 存储位置 | `orders` 表的数据存在 HDFS 的哪个目录 |
| 表名 → 分区信息 | `orders` 表按 `dt` 分区，有哪些分区目录 |

Analyzer 的解析过程：

```
Unresolved Relation: users
    │
    ├─ Catalog.lookupRelation("users")
    │     → Table: users (id: Long, name: String, age: Int, city: String)
    │
    ├─ 将 users 解析为带 Schema 的 Relation
    │
    └─ 校验列名：name 和 age 都存在 ✓
       校验类型：age > 18 中 age 是 Int, 18 可隐式转换 ✓
```

> **踩坑经验**：这是 SQL 查询的第一个报错点。如果你的 SQL 报错"cannot resolve 'xxx' given input columns"，说明 Analyzer 阶段没有找到这个列。检查是否拼写错误，或者表是否有这个列。

## 阶段二：Logical Optimization（逻辑优化）

Catalyst 通过**规则匹配**对逻辑计划做等价变换。以下是核心优化规则：

### 1. 谓词下推（Predicate Pushdown）

**谓词下推是所有优化规则中效果最明显的一个。**

```
优化前:                   优化后:
Filter(age > 18)         Filter(age > 18)
    │                         │
  Scan(users)              Scan(users)
  读全表数据               只读 age > 18 的数据
  (1000 行)               (200 行)

对 Parquet/ORC 更明显：
  Parquet 每个 RowGroup 包含 min/max 统计
  谓词下推后，Reader 跳过不符合条件的 RowGroup
  IO 大幅减少
```

谓词下推的三种场景：

| 场景 | 说明 | 效果 |
|------|------|------|
| 分区剪裁 | 过滤分区列，跳过无关分区目录 | 减少扫描的目录数 |
| RowGroup 过滤 | Parquet/ORC 基于 min/max 跳过行组 | 减少读取的行组 |
| 数据源下推 | JDBC 下推到数据库执行 WHERE | 减少数据库返回的数据 |

> **面试点**：谓词下推对列式存储（Parquet/ORC）为什么效果最好？因为列式存储文件内部按行组（RowGroup）组织，每个行组记录了每列的 min/max 统计值。下推的谓词可以和 min/max 比较，跳过不满足条件的整个行组。而行式存储（如 CSV）没有这个能力。

### 2. 列裁剪（Column Pruning）

**列裁剪只读取 SQL 中引用的列，是 IO 优化的另一个重要手段。**

```
原始表 users(id, name, age, city, phone, email, address, created_at)

SQL: SELECT name, age FROM users

优化前:                    优化后:
scan 所有 8 列           只 scan name, age 2 列
IO: 8 列全读              IO: 2 列
```

列裁剪在多表 Join 和子查询场景效果更显著：

```sql
-- 多表场景
SELECT u.name, o.amount
FROM users u JOIN orders o ON u.id = o.user_id
-- 优化后：
-- users: 只读 id, name
-- orders: 只读 user_id, amount
-- 即使两张表都有 100 列，也只读 2 列
```

### 3. 常量折叠（Constant Folding）

在编译期计算常量表达式，避免运行时重复计算。

```sql
-- 原始
SELECT * FROM orders WHERE amount > 100 + 50

-- 优化后（100 + 50 在编译期计算为 150）
SELECT * FROM orders WHERE amount > 150
```

同样适用于：

```sql
-- 原始
SELECT name, salary * (1 + 0.1) AS taxed_salary FROM employees

-- 优化后（1 + 0.1 = 1.1）
SELECT name, salary * 1.1 AS taxed_salary FROM employees
```

### 4. Join 重排序

Catalyst 会根据表的大小重排 Join 顺序，让最小的表先参与 Join。

```
假设：
  A: 1000 行  B: 100 行  C: 10 行
  A JOIN B JOIN C

优化前（从左到右）:
  (A JOIN B) JOIN C
  → A JOIN B 产生大量中间结果

优化后:
  (C JOIN B) JOIN A
  → C JOIN B 中间结果极小
```

> **面试点**：Join 重排序的前提是 `spark.sql.cbo.enabled = true`（需要收集表的统计信息）。否则 Catalyst 不知道表的大小，无法做出正确的重排。这也就是为什么需要定期执行 `ANALYZE TABLE`。

### 5. 投影合并

消除不必要的子查询和重复的投影操作。

```sql
-- 原始
SELECT name FROM (
  SELECT name, age FROM users
)

-- 优化后（合并 Select）
SELECT name FROM users
```

### 6. Null 传播

自动优化 null 相关的表达式。

```sql
-- 原始
SELECT amount + 0 FROM orders

-- 优化后（amount + 0 → amount）
SELECT amount FROM orders
```

### 7. 分区剪裁

当查询条件包含分区列时，自动跳过无关分区。

```sql
-- 假设 orders 按 dt 分区
SELECT * FROM orders WHERE dt = '2024-01-01'

-- 优化后：只扫描 dt=2024-01-01 这个目录
-- 如果表有 365 个分区目录，只读 1 个 → 减少 99.7% 的 IO
```

## 阶段三：Physical Planning（物理计划）

逻辑优化后，Catalyst 需要决定**怎么执行**——这就是物理计划的工作。

```scala
// Optimized Logical Plan 转为 1~N 个 Physical Plan
// 物理计划包含具体执行策略

// 例：A JOIN B
// 候选 Plan1: BroadcastHashJoin（如果 A < 10MB）
// 候选 Plan2: SortMergeJoin（默认）
// 候选 Plan3: ShuffleHashJoin

// 成本模型选最优：
// - BroadcastHashJoin: Cost = 0（无 Shuffle）
// - SortMergeJoin: Cost = Shuffle(A) + Shuffle(B) + Sort(A) + Sort(B) + Merge
// - 取 Cost 最小的 Plan
```

```scala
// 查看物理计划
df.explain("cost")
// == Optimized Logical Plan ==
// == Physical Plan ==
// *(2) HashAggregate(keys=[user_id], functions=[sum(amount)])
// +- Exchange hashpartitioning(user_id, 200)
//    +- *(1) HashAggregate(keys=[user_id], functions=[partial_sum(amount)])
//       +- *(1) FileScan parquet [user_id, amount]
```

物理计划的选择流程：

| 步骤 | 操作 | 说明 |
|------|------|------|
| 1 | 生成候选计划 | 对同一个逻辑计划生成多个物理实现 |
| 2 | 计算成本 | 每个候选估算 Shuffle、排序、扫描的代价 |
| 3 | 选择最优 | 选总成本最低的候选计划 |
| 4 | 生成 RDD | 将选中的物理计划转换为 RDD 执行 |

### 物理计划中的关键符号

在 `explain` 输出中，你会看到一些特殊符号：

| 符号 | 含义 | 示例 |
|------|------|------|
| `*` (星号) | WholeStageCodegen 生成的节点 | `*(1) Filter` |
| `+` (加号) | Exchange/Shuffle 边界 | 表示需要 Shuffle |
| `Exchange` | Shuffle 操作 | `Exchange hashpartitioning(key, 200)` |

## 阶段四：Code Generation（代码生成）

### WholeStageCodegen

Spark 2.0+ 的 Tungsten 引擎引入整阶段代码生成。这是 Spark SQL 比 Spark 1.x RDD API 快 2-5 倍的关键原因。

```
传统执行：
  Iterator[A] → map(A→B) → Iterator[B] → filter(B→Bool) → Iterator[B]
  → 每条记录都要经过多个虚函数调用（虚函数分派开销很大）

WholeStageCodegen：
  编译为单个 while 循环：
  while (hasNext()) {
    val a = next()
    val b = func1(a)   // map
    if (func2(b)) {    // filter
      output(b)
    }
  }
  → 消除所有中间迭代器和虚函数调用
```

```scala
// 查看代码生成
df.explain("codegen")
// Found 2 WholeStageCodegen subtrees:
// == Subtree 1 / 2 ==
// *(1) FileScan parquet...
// 
// == Subtree 2 / 2 ==
// *(2) HashAggregate...
//   Generated code:
//   /* 001 */ public Object generate(Object[] references) {
```

### 代码生成的优势

```
1. 消除虚函数调用（JIT 无法内联的虚函数）
2. 减少中间对象分配（减少 GC 压力）
3. 利用 CPU 寄存器存储中间变量
4. CPU L1/L2 缓存命中率更高
```

WholeStageCodegen 在 Join 场景的效果：

```
无 Codegen:
  Scan → 每行包装为 Row 对象 → Filter → 每行检查 → Project → 每行列裁剪
  → GC 压力大：大量短暂的 Row 对象

有 Codegen:
  编译为一个 while 循环：
  while (page.hasNext()) {
    row = page.getRow()
    if (row.getInt(1) > 18) {  // 内联的列访问
      writeRow(row.getInt(0), row.getString(2))
    }
  }
  → 无中间对象，直接访问列存数据
```

### Codegen 的局限性

不是所有场景都能从 Codegen 中受益：

| 场景 | Codegen 效果 | 原因 |
|------|-------------|------|
| 简单过滤 + 投影 | 极好 | 可合并为单 while 循环 |
| 复杂聚合 | 好 | HashAggregate 可代码生成 |
| UDF | 差 | UDF 内部是黑盒，无法内联 |
| Shuffle 边界 | 无影响 | 网络传输是瓶颈 |

## 查看执行计划

```scala
// 常用 explain 模式
df.explain("simple")     // 只显示物理计划（简洁）
df.explain("extended")    // 逻辑计划 + 物理计划（完整）
df.explain("cost")        // 含成本估算
df.explain("codegen")     // 含生成的 Java 代码

// 查看 QueryExecution
df.queryExecution.logical          // 未优化的逻辑计划
df.queryExecution.optimizedPlan    // 优化后的逻辑计划
df.queryExecution.executedPlan     // 执行的物理计划
```

## 面试高频考点

### Q: Catalyst 为什么比 Hive SQL 快？

Hive SQL 转换为 MapReduce 任务：每个算子一个 MR Job，中间结果落磁盘。Catalyst 对所有算子做全局优化（谓词下推、列裁剪等），编译为单个 DAG，中间结果在内存流水线执行。再加上 WholeStageCodegen 的编译优化，性能差距可达 10-100 倍。

### Q: Catalyst 优化器的核心设计？

1. **树结构（Tree）**：所有 Plan 和 Expression 都是树节点
2. **规则（Rule）**：`Rule[Tree]` 模式匹配 → 等价变换
3. **多批次（Batches）**：规则分批次重复应用直到收敛

### Q: WholeStageCodegen 如何提升性能？

将多个算子编译为单个 while 循环，消除中间迭代器的虚函数调用，减少对象分配，同时让 JIT 编译器更好地内联和寄存器分配。简单说：把"每行处理"变成了"批处理循环"。

### Q: 谓词下推为什么对 Parquet 效果最好？

因为 Parquet 的 RowGroup 存储了每列的 min/max 统计信息，谓词下推后可以直接在 RowGroup 级别过滤。CSV 等行式存储无法做到这点。

### Q: 如何判断 Catalyst 优化的效果？

用 `df.explain(true)` 查看优化前后的逻辑计划，对比谓词是否被下推、列是否被裁剪。如果 Filter 在 Scan 之上（而不是内部），说明没有成功下推。

## 小结

| 阶段 | 输入 | 输出 | 核心规则 |
|------|------|------|---------|
| Analysis | SQL AST | Resolved Plan | Catalog 校验表名列名 |
| Optimizer | Resolved Plan | Optimized Plan | 谓词下推、列裁剪、常量折叠、Join 重排 |
| Planner | Optimized Plan | Physical Plan | 成本模型选择 BHJ/SMJ/SHJ |
| CodeGen | Physical Plan | Java 字节码 | WholeStageCodegen 消除虚函数 |
