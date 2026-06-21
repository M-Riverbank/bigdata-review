# Spark SQL — UDF、UDAF 与自定义函数

## 为什么需要自定义函数

Spark SQL 内置了 200+ 函数，覆盖了大部分常见的数据处理需求。但在实际生产环境中，总有一些业务逻辑是内置函数无法表达的：

- **复杂文本解析**：提取特定格式的日志字段，正则匹配复杂模式
- **调用外部服务**：根据 IP 查询地理位置，调用 ML 模型做预测
- **专有业务逻辑**：公司的计费规则、风控评分算法
- **自定义聚合**：加权平均、几何平均等内置函数不支持的计算

> **面试点**：面试官问"什么场景用 UDF？"答案是"只有内置函数无法完成时才用"。因为 UDF 有性能损失，能用 Spark SQL 内置函数尽量用内置函数。

## UDF（用户自定义函数）

### 注册和使用

```scala
import org.apache.spark.sql.functions.udf

// 在 DataFrame API 中使用
val toUpperCase = udf((s: String) => s.toUpperCase)
df.select(toUpperCase($"name").alias("name_upper"))

// 在 SQL 中使用
spark.udf.register("toUpperCase", (s: String) => s.toUpperCase)
spark.sql("SELECT toUpperCase(name) FROM people").show()
```

### 复杂 UDF 示例

```scala
// 多参数 UDF
val calculateScore = udf(
  (score: Double, weight: Double, bonus: Double) => {
    score * weight + bonus
  }
)

df.select(calculateScore($"score", $"weight", $"bonus"))

// 返回复杂类型
val parseLog = udf((line: String) => {
  val parts = line.split(",")
  (parts(0), parts(1).toDouble, parts(2))  // 返回 Tuple3
})

// 返回 null 处理
val safeParse = udf((s: String) => {
  try {
    Some(s.toInt)
  } catch {
    case _: NumberFormatException => None  // 返回 null
  }
})
```

### UDF 的注册方式对比

| 注册方式 | API 中使用 | SQL 中使用 | 适用场景 |
|---------|-----------|-----------|---------|
| `udf(...)` 函数 | ✅ 直接调用 | ❌ | DataFrame API 编程 |
| `spark.udf.register(...)` | ✅ 直接调用 | ✅ `spark.sql()` | 两边都需要用 |
| `spark.sql("CREATE FUNCTION ...")` | ❌ | ✅ | 仅 SQL 场景 |

### UDF 性能问题

```scala
// ❌ 注意：UDF 会跳出 Catalyst 优化
// 因为 Catalyst 不了解 UDF 内部的逻辑
// → 无法做谓词下推、列裁剪等优化

// ✅ 如果逻辑可以用内置函数实现，优先用内置函数
df.select(upper($"name"))  // 内置函数，Catalyst 可优化
df.select(toUpperCase($"name"))  // UDF，Catalyst 无法优化

// UDF 中的序列化开销
// 每行数据需要在 JVM 对象和 Tungsten 格式之间转换
```

**UDF vs 内置函数的性能对比：**

| 对比维度 | 内置函数 | UDF |
|---------|---------|-----|
| Catalyst 优化 | 可优化（谓词下推等） | 不可优化（黑盒） |
| 序列化 | 无（直接操作 Tungsten） | 有（JVM ↔ Tungsten） |
| 每行处理 | 向量化/批处理 | 逐行处理 |
| 代码生成 | WholeStageCodegen | 无法内联 |
| 性能差异 | 基准 | 慢 2-10 倍 |

> **踩坑经验**：UDF 的序列化开销往往比业务逻辑本身的执行时间还要大。比如一个简单的 `(s: String) => s.trim`，实际执行时间可能 90% 花在序列化上，10% 花在 trim 上。如果一定要用 UDF，考虑用 Spark 3.0+ 的 **Pandas UDF**（向量化 UDF），它通过 Arrow 做零拷贝数据交换，性能接近内置函数。

**判断是否能用内置函数替代的 checklist：**

```
┌─ 你的逻辑能否用 when/otherwise 实现？→ 是 → 用 when/otherwise
├─ 你的逻辑能否用 regexp_extract/regexp_replace 实现？→ 是 → 用正则函数
├─ 你的逻辑能否用 JSON 函数（get_json_object）实现？→ 是 → 用 JSON 函数
├─ 你的逻辑能否用日期函数实现？→ 是 → 用日期函数
└─ 以上都不行 → 使用 UDF
```

## UDAF（用户自定义聚合函数）

UDAF 输入的是一组行，输出的是单行结果。最常见的例子就是 `SUM`、`AVG`、`COUNT`。如果你需要一个内置函数中没有的聚合逻辑，就需要自定义 UDAF。

### Aggregator API（强类型，推荐）

Spark 2.x+ 推荐使用 `Aggregator` API，它是类型安全的，代码可读性更好。

```scala
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders}

// 自定义：计算加权平均值
case class WeightedAverage(
  sum: Double = 0.0,
  weightSum: Double = 0.0
) {
  def value: Double = if (weightSum == 0) 0 else sum / weightSum
}

object WeightedAvg extends Aggregator[(Double, Double), WeightedAverage, Double] {
  // 初始值
  def zero: WeightedAverage = WeightedAverage()

  // 分区内合并
  def reduce(b: WeightedAverage, a: (Double, Double)): WeightedAverage =
    WeightedAverage(b.sum + a._1 * a._2, b.weightSum + a._2)

  // 分区间合并
  def merge(b1: WeightedAverage, b2: WeightedAverage): WeightedAverage =
    WeightedAverage(b1.sum + b2.sum, b1.weightSum + b2.weightSum)

  // 最终结果
  def finish(reduction: WeightedAverage): Double = reduction.value

  // 编码器（序列化）
  def bufferEncoder: Encoder[WeightedAverage] = Encoders.product
  def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

// 使用
val weightedAvg = WeightedAvg.toColumn.name("weighted_avg")
df.select(weightedAvg)
```

### Aggregator 的 5 个方法

| 方法 | 说明 | 执行阶段 |
|------|------|---------|
| `zero` | 初始值（缓冲区的起始状态） | 每个分区开始时 |
| `reduce` | 将一行数据合并到缓冲区 | Map 端（分区内） |
| `merge` | 将两个缓冲区合并 | Reduce 端（分区间） |
| `finish` | 从缓冲区计算最终结果 | 完成后 |
| `bufferEncoder` | 缓冲区序列化器 | 配置 |
| `outputEncoder` | 输出序列化器 | 配置 |

### UserDefinedAggregateFunction（旧 API）

Spark 1.x 时代的旧 API，类型不安全（所有类型都是 `DataType`），但在某些支持场景下仍可使用。

```scala
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

// 无类型 UDAF（Spark 3.x 推荐用 Aggregator）
class GeometricMean extends UserDefinedAggregateFunction {
  def inputSchema: StructType = StructType(StructField("value", DoubleType) :: Nil)
  def bufferSchema: StructType = StructType(
    StructField("product", DoubleType) ::
    StructField("count", LongType) :: Nil
  )
  def dataType: DataType = DoubleType
  def deterministic: Boolean = true

  def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.0  // product = 1
    buffer(1) = 0L   // count = 0
  }

  def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      buffer(0) = buffer.getDouble(0) * input.getDouble(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getDouble(0) * buffer2.getDouble(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  def evaluate(buffer: Row): Double = {
    math.pow(buffer.getDouble(0), 1.0 / buffer.getLong(1))
  }
}

// 注册使用
spark.udf.register("geometric_mean", new GeometricMean)
spark.sql("SELECT geometric_mean(amount) FROM orders").show()
```

### Aggregator vs UserDefinedAggregateFunction

| 对比维度 | Aggregator（新 API） | UserDefinedAggregateFunction（旧 API） |
|---------|-------------------|---------------------------------------|
| 类型安全 | ✅ 强类型 | ❌ Runtime 类型 |
| API 复杂度 | 中等 | 较高 |
| Encoder | ✅ 自动生成 | ❌ 手动处理 |
| SQL 中使用 | 需要 `toColumn` | ✅ 直接注册 |
| 推荐度 | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ |

### UDAF 的执行流程

```
UDAF 在 Spark 中分三步执行：

1. Partial（部分聚合）
   每个分区内：reduce(b, a) → 中间结果 buffer
   在 Map 端做预聚合（类似 combiner）

2. Final（最终聚合）
   分区间：merge(buffer1, buffer2) → 最终 buffer

3. Evaluation（结果计算）
   finish(buffer) → 最终值

Partial 阶段的预聚合大幅减少 Shuffle 数据量！
```

以 `WeightedAvg` 为例的执行流程：

```
数据分布：
  Partition 1: [("苹果", 5.0, 0.2), ("苹果", 3.0, 0.8)]
  Partition 2: [("苹果", 4.0, 0.5), ("苹果", 6.0, 0.5)]

Partition 1 的 reduce：
  start: WeightedAverage(0, 0)
  row1: WeightedAverage(5.0*0.2, 0.2) = WeightedAverage(1.0, 0.2)
  row2: WeightedAverage(1.0 + 3.0*0.8, 0.2+0.8) = WeightedAverage(3.4, 1.0)
  → 输出 buffer: WeightedAverage(3.4, 1.0)

Partition 2 的 reduce：
  → 输出 buffer: WeightedAverage(5.0, 1.0)

Merge（跨分区合并）：
  merge(b1, b2) → WeightedAverage(3.4+5.0, 1.0+1.0) = WeightedAverage(8.4, 2.0)

Finish：
  finish: 8.4 / 2.0 = 4.2
  → 加权平均 = 4.2
```

## UDTF（用户自定义表生成函数）— PySpark

UDTF 是一行输入、多行输出的函数，和 `EXPLODE` 类似但更灵活。

```python
# PySpark 支持 UDTF（Spark 3.5+）
from pyspark.sql.functions import udtf

@udtf(returnType="word: string, count: int")
def word_count(text: str):
    words = text.split()
    for w in set(words):
        yield (w, words.count(w))

spark.sql("SELECT * FROM word_count('hello hello spark')")
# +-------+-----+
# |  word |count|
# +-------+-----+
# | hello |  2  |
# | spark |  1  |
# +-------+-----+
```

## Pandas UDF（向量化 UDF）

Pandas UDF（也称为向量化 UDF）是 PySpark 中性能最好的自定义函数方案。它通过 Apache Arrow 实现零拷贝数据交换，避免了逐行序列化的开销。

```python
# PySpark 的向量化 UDF，性能比普通 UDF 提升 ~10x
# 基于 Apache Arrow 做零拷贝数据交换

from pyspark.sql.functions import pandas_udf
import pandas as pd

# Scalar UDF（逐行向量化）
@pandas_udf("double")
def calculate_tax(amount: pd.Series) -> pd.Series:
    return amount * 0.1

# Grouped UDF（分组聚合）
@pandas_udf("double")
def weighted_mean(v: pd.Series, w: pd.Series) -> float:
    return (v * w).sum() / w.sum()
```

### 各类 UDF 的性能对比

| UDF 类型 | 性能 | 语言 | 数据交换方式 | 适用场景 |
|---------|------|------|------------|---------|
| Scala UDF | ⭐⭐ | Scala | 逐行 Java 序列化 | 简单转换 |
| Python UDF | ⭐ | Python | 逐行 Pickle 序列化 | 简单 Python 逻辑 |
| Pandas UDF (Scalar) | ⭐⭐⭐⭐ | Python | 批处理 Arrow | 复杂 Python 逻辑 |
| Pandas UDF (Grouped) | ⭐⭐⭐⭐ | Python | 批处理 Arrow | 分组聚合 |
| Pandas UDF (Grouped Map) | ⭐⭐⭐⭐⭐ | Python | 全组 Arrow | 每组内复杂处理 |

## 面试高频考点

### Q: UDF 为什么比内置函数慢？

1. **跳出 Catalyst 优化**：UDF 内部的逻辑 Catalyst 无法理解
2. **序列化开销**：每行数据在 JVM 对象和 Tungsten 格式间转换
3. **每行调用**：逐行处理，无法利用向量化

### Q: UDAF 和普通 UDF 的区别？

UDF 输入一行输出一行（逐行转换）。UDAF 输入多行输出一行（分组聚合）。UDAF 内部可以分 Partial/Final 两阶段执行，在 Map 端做预聚合，显著减少 Shuffle 数据量。

### Q: 什么时候应该用 UDF 什么时候用 Spark SQL 内置函数？

优先使用内置函数。只有内置函数无法表达的逻辑才用 UDF（如复杂文本处理、正则匹配、调用外部 API）。如果必须用 UDF，优先在 Scala/Java 中实现（性能更好），实在不行才用 Python UDF。

### Q: Aggregator 的四个核心方法是什么？

`zero`（初始化）、`reduce`（分区内合并）、`merge`（分区间合并）、`finish`（输出结果）。理解这四个方法就基本掌握了 UDAF 的核心。

## 小结

| 函数类型 | 输入 | 输出 | 场景 |
|---------|------|------|------|
| UDF | 一行 | 一行 | 复杂转换逻辑 |
| UDAF | 多行 | 一行 | 自定义聚合逻辑 |
| UDTF | 一行 | 多行 | 行转列拆分 |
| Pandas UDF | 向量 | 向量 | 高性能 PySpark 处理 |
| Aggregator | 强类型 | 强类型 | Scala 类型安全聚合 |

**UDF 使用决策树：**

```
能否用 Spark 内置函数实现？
  ├─ 是 → 用内置函数（性能最好）
  └─ 否 → 必需要用 UDF
         ├─ Scala/Java 项目 → Scala UDF 或 Aggregator
         ├─ PySpark 项目 → Pandas UDF（优先）/ Python UDF
         └─ SQL 项目 → spark.udf.register() 注册后使用
```
