# Spark SQL 窗口函数详解

## 为什么需要窗口函数

在日常数据分析中，我们经常遇到这样的需求：**既要保留每一行的原始数据，又要计算分组内的聚合值**。

传统的 `GROUP BY` 会把多行聚合为一行，丢失了数据的明细。比如：

```sql
-- GROUP BY 后，我们能看到每个部门的平均工资，但看不到每个人的工资和部门平均的关系
SELECT department, AVG(salary) FROM employees GROUP BY department;
```

窗口函数解决了这个矛盾——它在不折叠行数的情况下，为每一行计算分组范围内的聚合值。

```
┌─────────────────────────────────────────┐
│  普通聚合 (GROUP BY)                     │
│  多行 → 一行                             │
│  SUM(sales) GROUP BY region → 每region一行 │
├─────────────────────────────────────────┤
│  窗口函数 (OVER)                         │
│  多行 → 多行（每行都有计算结果）           │
│  SUM(sales) OVER (PARTITION BY region)  │
│  → 每行保留，附带该region的汇总值         │
└─────────────────────────────────────────┘
```

> **面试点**：面试中"说一下你对窗口函数的理解"是一个高频题。建议的回答逻辑是：窗口函数在**不减少行数**的前提下，对分区内的行进行聚合/排序/偏移操作。它在分组明细的场景（如"每个部门内工资排名"）中无可替代。

### 窗口函数的典型应用场景

| 场景 | 描述 | 不用窗口函数的做法 |
|------|------|-----------------|
| TOP N 问题 | 每个部门工资前三 | 自 Join + 子查询，复杂低效 |
| 同比环比 | 计算 DAU 环比变化 | 自 Join，需要处理空值 |
| 累计求和 | 每日销售额累计值 | 自 Join 聚合，O(n^2) 复杂度 |
| 移动平均 | 股票 7 日均线 | 多行 Join 平均 |
| 分组去重 | 按用户取最新一条记录 | Row_number + 子查询 |

## 窗口函数语法

```sql
window_function(expr) OVER (
  [PARTITION BY partition_col1, partition_col2, ...]
  [ORDER BY order_col1 [ASC|DESC], order_col2 [ASC|DESC], ...]
  [ROWS/RANGE BETWEEN frame_start AND frame_end]
)
```

### 三个核心组成部分

| 子句 | 作用 | 必需？ |
|------|------|--------|
| `PARTITION BY` | 将数据分成多个分区，每个分区间独立计算 | 否 |
| `ORDER BY` | 在每个分区内对行排序 | 部分函数需要 |
| `ROWS/RANGE` | 定义帧（Frame）范围，即当前行参与计算的行集 | 否，有默认值 |

> **踩坑经验**：很多初学者以为 `PARTITION BY` 不是必需的。确实，不加 `PARTITION BY` 不会报错，但此时窗口会对**整个结果集**进行计算。如果数据量很大（比如 1 亿行），这个窗口函数的计算量会非常恐怖。在大多数业务场景下，你都需要加上 `PARTITION BY` 来限定窗口范围。

### 各参数的作用示意图

```
PARTITION BY department
┌──────────────────────────────────────────┐
│  技术部（独立计算）                       │
│  │ ORDER BY salary DESC                  │
│  ├── 张三 30000  ← 当前行                │
│  │      ↑ FRAME: ROWS BETWEEN ...        │
│  ├── 李四 25000                          │
│  └── 王五 18000                          │
├──────────────────────────────────────────┤
│  行政部（独立计算）                       │
│  ├── 赵六 15000                          │
│  └── 钱七 12000                          │
└──────────────────────────────────────────┘
```

## 常用窗口函数分类

### 1. 排名函数

| 函数 | 说明 | 样例输出 |
|------|------|---------|
| `ROW_NUMBER()` | 从1开始的连续序号，相同值随机分配 | 1,2,3,4 |
| `RANK()` | 相同值同排名，后面排名跳过 | 1,2,2,4 |
| `DENSE_RANK()` | 相同值同排名，后面排名不跳过 | 1,2,2,3 |
| `NTILE(n)` | 将数据均匀分成 n 个桶 | 1,1,2,2,3,3 |

```sql
-- 示例：各部门内按工资排名
SELECT
  name,
  department,
  salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn,
  RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS rk,
  DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dr
FROM employees;
```

假设数据：

```
name   department  salary  rn  rk  dr
张三   技术部      30000   1   1   1
李四   技术部      25000   2   2   2
王五   技术部      25000   3   2   2   （与李四同薪）
赵六   技术部      18000   4   4   3
```

> **面试点**：`ROW_NUMBER`、`RANK`、`DENSE_RANK` 三者的区别是面试基础题。记忆口诀："ROW 连续，RANK 跳号，DENSE 不跳"。

> **踩坑经验**：`ROW_NUMBER()` 在 **ORDER BY 字段值相同**时，返回的行顺序是不确定的！如果订单时间相同，每次查询的行号可能不一样。如果需要确定性行为，ORDER BY 要加更多列确保唯一排序。

### 2. 偏移函数

| 函数 | 说明 |
|------|------|
| `LAG(col, n, default)` | 向上偏移 n 行，获取前面的值 |
| `LEAD(col, n, default)` | 向下偏移 n 行，获取后面的值 |
| `FIRST_VALUE(col)` | 帧内第一个值 |
| `LAST_VALUE(col)` | 帧内最后一个值 |

```sql
-- 示例：计算每日活跃用户DAU的环比变化
SELECT
  dt,
  dau,
  LAG(dau, 1) OVER (ORDER BY dt) AS prev_dau,
  ROUND((dau - LAG(dau, 1) OVER (ORDER BY dt)) * 100.0
    / LAG(dau, 1) OVER (ORDER BY dt), 2) AS change_pct
FROM dau_table
ORDER BY dt;
```

输出示例：

```
dt         dau    prev_dau  change_pct
2024-01-01 10000  null      null
2024-01-02 12000  10000     20.00
2024-01-03 11000  12000     -8.33
2024-01-04 15000  11000     36.36
```

> **踩坑经验**：`LAG` 和 `LEAD` 的默认值处理。如果偏移超出了数据的范围，默认返回 `null`。如果你希望返回一个默认值而不是 null，可以传入第三个参数：`LAG(dau, 1, 0)`。在计算比率时特别要处理 null（或者用 `COALESCE`）。

`FIRST_VALUE` 和 `LAST_VALUE` 的进阶用法：

```sql
-- 计算每个用户的首单金额和末单金额
SELECT
  user_id,
  order_date,
  amount,
  FIRST_VALUE(amount) OVER (
    PARTITION BY user_id
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS first_order_amount,
  LAST_VALUE(amount) OVER (
    PARTITION BY user_id
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  ) AS last_order_amount
FROM orders;
```

> **踩坑经验（非常重要）**：`LAST_VALUE` 的默认帧范围是 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`，所以如果不指定帧范围，它会返回当前行的值（因为当前行就是帧内最后一行），而不是分区内的最后一行。**必须显式指定 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`** 才能获取分区的真正最后一行。

### 3. 聚合窗口函数

普通的聚合函数（`SUM`、`AVG`、`COUNT`、`MAX`、`MIN`）都可以作为窗口函数使用，只需加上 `OVER()` 子句。

```sql
-- 累计销售额（Running Total）
SELECT
  order_date,
  amount,
  SUM(amount) OVER (
    ORDER BY order_date
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_sum
FROM orders;
```

```sql
-- 移动平均（Moving Average）
SELECT
  order_date,
  amount,
  AVG(amount) OVER (
    ORDER BY order_date
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS ma_7day
FROM orders;
```

```sql
-- 各组占比
SELECT
  department,
  employee,
  salary,
  salary / SUM(salary) OVER (PARTITION BY department) AS pct_of_dept
FROM employees;
```

```sql
-- 和最大值差距
SELECT
  department,
  employee,
  salary,
  MAX(salary) OVER (PARTITION BY department) - salary AS gap_to_top
FROM employees;
```

## 窗口帧（Frame）详解

帧定义了当前行参与计算的行范围。理解帧是掌握窗口函数的关键。

### ROWS vs RANGE

| Frame 类型 | 说明 | 示例 |
|------------|------|------|
| `ROWS` | **物理行数**范围，基于行号偏移 | 当前行前 3 行到后 3 行 |
| `RANGE` | **逻辑值**范围，ORDER BY 值相同的行视为一组 | 当前值 ± 7 天的所有行 |

> **面试点**：`ROWS` 和 `RANGE` 的区别？ROWS 固定偏移行数，RANGE 根据 ORDER BY 列的值决定范围。举例：ORDER BY salary，当前行 salary=5000。ROWS BETWEEN 1 PRECEDING → 物理上一行；RANGE BETWEEN 1000 PRECEDING → 所有 salary 在 4000-5000 之间的行。

### 常见帧边界

```sql
-- 从分区开始到当前行（默认）
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

-- 从分区开始到分区结束
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

-- 当前行前后各3行
ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING

-- 基于值的范围（RANGE 模式）
-- 假设 ORDER BY salary，值=5000
RANGE BETWEEN 1000 PRECEDING AND 1000 FOLLOWING
-- 包含 salary 在 [4000, 6000] 之间的所有行
```

帧边界的特殊值：

| 特殊值 | 含义 |
|--------|------|
| `UNBOUNDED PRECEDING` | 分区第一行 |
| `UNBOUNDED FOLLOWING` | 分区最后一行 |
| `CURRENT ROW` | 当前行 |

### 默认帧

| 场景 | 默认帧 |
|------|--------|
| 有 `ORDER BY` | `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` |
| 无 `ORDER BY` | `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` |

> **踩坑经验**：默认帧是最常见的"误解源"。很多人不知道 `SUM(...) OVER (PARTITION BY dept ORDER BY salary)` 的默认帧不是全部分区，而是 `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`。这意味着它计算的是**累计求和**，而不是分区总和。如果需要分区总和，要么不加 ORDER BY，要么显式指定帧范围。

### 帧的完整示例

```sql
-- 不同帧对结果的影响
SELECT
  dt,
  amount,
  SUM(amount) OVER (ORDER BY dt) AS default_frame,          -- 累计（默认）
  SUM(amount) OVER (ORDER BY dt 
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS rows_frame,  -- 与默认相同
  SUM(amount) OVER (ORDER BY dt 
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sliding_3,  -- 滑动窗口
  SUM(amount) OVER (ORDER BY dt 
    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS total,  -- 全部
  AVG(amount) OVER (ORDER BY dt 
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS ma_7       -- 7日均线
FROM daily_sales;
```

## DataFrame API 中的窗口函数

```scala
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

// 定义窗口规格
val windowSpec = Window
  .partitionBy("department")
  .orderBy(col("salary").desc)
  .rowsBetween(Window.unboundedPreceding, Window.currentRow)

// TOP N 查询
df.withColumn("rn", row_number().over(windowSpec))
  .where("rn <= 3")

// 复用 WindowSpec 减少 Shuffle
val sharedSpec = Window.partitionBy("department").orderBy("salary")

val result = df
  .withColumn("row_num", row_number().over(sharedSpec))
  .withColumn("avg_salary", avg("salary").over(sharedSpec))
  .withColumn("salary_pct", col("salary") / sum("salary").over(sharedSpec))
```

## 性能调优建议

### 核心原则

1. **减少 PARTITION BY 的基数**：分区列的值域太大（如 user_id）会导致 Shuffle 压力巨大。如果是去重场景，可以考虑先粗粒度聚合再去重。

2. **合理使用 ORDER BY**：没有 ORDER BY 时窗口函数不保证顺序。但如果你的窗口函数不需要排序（如计算分组总和），就不要加 ORDER BY，否则会触发不必要的排序操作。

3. **避免多个不同窗口定义**：相同的 `PARTITION BY + ORDER BY` 共享一个 WindowSpec，减少 Shuffle。Spark 会合并相同窗口定义的多个函数。

```scala
// Spark DataFrame API — 复用 WindowSpec
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("department").orderBy("salary")
val result = df
  .withColumn("rn", row_number().over(windowSpec))
  .withColumn("avg_salary", avg("salary").over(windowSpec))
```

### 执行计划分析

```scala
// 查看窗口函数的执行计划
df.where("rn <= 3").explain(true)

// 物理计划中会看到：
// Window [row_number()...], [department#1], [salary#4 DESC]
// +- Sort [department#1 ASC, salary#4 DESC], false, 0
//    +- Exchange hashpartitioning(department#1, 200)
//       +- ...
```

从执行计划可以看到，窗口函数的执行顺序是：

```
Shuffle → Sort → Window → Post-filter
```

这意味着窗口函数至少会触发一次 Shuffle（因为 PARTITION BY 需要数据重分布）和一次 Sort（因为 ORDER BY 需要排序）。

### 应对大数据量窗口函数的策略

| 策略 | 适用场景 | 做法 |
|------|---------|------|
| 分区列优化 | 分区列基数过大 | 用复合列降低基数 |
| 预处理 | 计算量大且可预计算 | 先用 GROUP BY 粗聚合 |
| AQE | Spark 3.0+ | 开启自适应执行优化 |
| 合适的分区数 | Shuffle 分区不合适 | 调 `spark.sql.shuffle.partitions` |

## 面试高频考点

### Q: ROW_NUMBER、RANK、DENSE_RANK 的区别是什么？

ROW_NUMBER 连续编号；RANK 并列时跳号（1,1,3）；DENSE_RANK 并列时不跳号（1,1,2）。

### Q: LAG 和 LEAD 的默认值是什么？

默认返回 null。可以通过第三个参数设置默认值。

### Q: 窗口函数的执行顺序是怎样的？

Shuffle（PARTITION BY 数据重分）→ Sort（ORDER BY 排序）→ Window（计算窗口函数）。这比普通聚合多了一个排序步骤。

### Q: 窗口函数的性能瓶颈在哪里？

主要在 Shuffle 和 Sort 阶段。PARTITION BY 列的数据倾斜会导致个别分区数据量过大，计算时间远超其他分区。

### Q: 什么是帧（Frame）？

帧定义了窗口函数计算时考虑的行范围。默认在有 ORDER BY 时是 RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW，无 ORDER BY 时是整个分区。

## 小结

| 场景 | 推荐窗口函数 | 注意事项 |
|------|-------------|---------|
| 排名/TOP N | ROW_NUMBER, RANK | 相同值用 RANK，依次编号用 ROW_NUMBER |
| 同比/环比 | LAG, LEAD | 注意默认 null 值处理 |
| 累计值 | SUM + ROWS UNBOUNDED PRECEDING | 注意默认帧 |
| 移动平均 | AVG + ROWS n PRECEDING | n 过大时考虑近似 |
| 分组取首/末 | FIRST_VALUE, LAST_VALUE | LAST_VALUE 必须指定全部帧 |
| 分组占比 | SUM OVER (PARTITION BY) | 避免 ORDER BY 影响帧范围 |
