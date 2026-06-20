# Spark SQL 窗口函数详解

## 什么是窗口函数

窗口函数（Window Function）是 Spark SQL 中最强大的分析工具之一。与普通聚合函数不同，窗口函数**不会将多行折叠成一行**，而是在每一行上保留原始数据的同时进行聚合计算。

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

## 常用窗口函数分类

### 1. 排名函数

| 函数 | 说明 |
|------|------|
| `ROW_NUMBER()` | 从1开始的连续序号，相同值随机分配 |
| `RANK()` | 相同值同排名，后面排名跳过（1,2,2,4） |
| `DENSE_RANK()` | 相同值同排名，后面排名不跳过（1,2,2,3） |
| `NTILE(n)` | 将数据均匀分成 n 个桶 |

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

### 3. 聚合窗口函数

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

## 窗口帧（Frame）详解

帧定义了当前行参与计算的行范围。

| Frame 类型 | 说明 |
|------------|------|
| `ROWS` | 物理行数范围 |
| `RANGE` | 逻辑值范围（相同的 ORDER BY 值视为同一组） |

### 常见帧边界

```sql
-- 从分区开始到当前行（默认）
ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW

-- 从分区开始到分区结束
ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING

-- 当前行前后各3行
ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING
```

## 面试高频考点

### Q: ROW_NUMBER vs RANK vs DENSE_RANK 的区别？

| 函数 | 相同值处理 | 后续排名 |
|------|-----------|---------|
| ROW_NUMBER | 随机分配不同序号 | 连续 |
| RANK | 同排名 | 跳过（1,2,2,4） |
| DENSE_RANK | 同排名 | 不跳过（1,2,2,3） |

### Q: ROWS 和 RANGE 的区别？

- `ROWS` 基于**物理行数**，无论 ORDER BY 列值是否相同
- `RANGE` 基于**逻辑值**，相同的 ORDER BY 值被视为同一行
- 当 ORDER BY 列有唯一值时，`ROWS` 和 `RANGE` 行为一致

```sql
-- 示例说明 ROWS vs RANGE
-- 数据: val=10, 10, 20
-- SUM() OVER (ORDER BY val ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
--   → 10, 20, 40（每行独立累加）

-- SUM() OVER (ORDER BY val RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
--   → 20, 20, 60（相同val值一起算）
```

### Q: 窗口函数与 GROUP BY 的执行顺序？

窗口函数在 **WHERE、GROUP BY、HAVING 之后执行**，在 ORDER BY、LIMIT 之前执行。这意味：
1. 不能用窗口函数的结果在 WHERE 中过滤（需要用子查询）
2. 窗口函数可以访问 GROUP BY 后的结果集

```sql
-- ❌ 错误：窗口函数不能在 WHERE 中使用
SELECT * FROM orders
WHERE ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) = 1;

-- ✅ 正确：用子查询
SELECT * FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY dt DESC) AS rn
  FROM orders
) WHERE rn = 1;
```

## 性能调优建议

1. **减少 PARTITION BY 的基数**：分区过多会导致 Shuffle 压力大
2. **合理使用 ORDER BY**：没有 ORDER BY 时窗口函数没有顺序保证
3. **避免多个不同窗口定义**：相同的 `PARTITION BY + ORDER BY` 共享一个 WindowSpec，减少 Shuffle

```scala
// Spark DataFrame API — 复用 WindowSpec
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("department").orderBy("salary")
val result = df
  .withColumn("rn", row_number().over(windowSpec))
  .withColumn("avg_salary", avg("salary").over(windowSpec))
```

## 小结

| 场景 | 推荐窗口函数 |
|------|-------------|
| 排名/TOP N | ROW_NUMBER, RANK |
| 同比/环比 | LAG, LEAD |
| 累计值 | SUM + ROWS UNBOUNDED PRECEDING |
| 移动平均 | AVG + ROWS n PRECEDING |
| 分组取分组内第一条/最后一条 | FIRST_VALUE, LAST_VALUE |
