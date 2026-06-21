# Spark MLlib — Pipeline 与工作流

## ML Pipeline 设计理念

### 为什么需要 Pipeline？

在实际的机器学习项目中，我们通常不会直接拿着原始数据就去训练模型。中间往往要经历：数据清洗 -> 特征编码 -> 特征组装 -> 特征缩放 -> 模型训练 -> 模型评估 这一整套流程。如果每个步骤都手写代码分散在多个函数里，会带来几个问题：

| 问题 | 具体表现 | 后果 |
|------|---------|------|
| 代码耦合 | 特征处理代码散落各处 | 维护困难，改一处可能影响所有 |
| 特征泄露 | 测试集的数据不小心参与了训练集的统计计算 | 模型评估结果虚高 |
| 线上线下不一致 | 开发环境手动做了 5 步特征处理，线上部署时漏了 1 步 | 模型效果暴跌 |
| 重复代码 | 每个模型都要写一遍相同的特征处理 | 开发效率低 |

**Spark ML Pipeline 就是为这些痛点而生的**。它的设计借鉴了 scikit-learn 的 Pipeline 概念，将**数据处理**和**模型训练**组织为一个有向无环图（DAG）。

但和 scikit-learn 的一个关键区别是：Spark 的 Pipeline 是**分布式**的，每个 Stage 都在集群上并行执行，可以处理 TB 甚至 PB 级的数据。

```
原始数据
    │
    ▼
Transformer 1: StringIndexer
    │  类别→索引转换
    ▼
Transformer 2: VectorAssembler
    │  多列→特征向量
    ▼
Transformer 3: StandardScaler
    │  特征标准化
    ▼
Estimator: RandomForestClassifier
    │  模型训练
    ▼
PipelineModel (已训练)
    │
    ▼
预测结果
```

### Pipeline 的两阶段设计

理解 Pipeline 的核心在于它的**两阶段设计**：

1. **`fit()` 阶段**：传入训练数据，所有 Transformer 学习参数（如 StringIndexer 构建词典、StandardScaler 计算均值和标准差），最终 Estimator 训练模型。返回一个 PipelineModel。
2. **`transform()` 阶段**：传入测试数据或新来的数据，PipelineModel 用 `fit()` 阶段学到的参数，对新数据做同样的转换和预测。

这种设计的核心思想是：**训练时的统计信息被"固化"在 PipelineModel 中**，保证了训练和预测时对数据的处理逻辑完全一致。

## Pipeline 组件

### Transformer（转换器）

Transformer 是 Pipeline 的"工人"，它的职责是对数据进行 **转换**：

```scala
import org.apache.spark.ml.Transformer

// 常见的 Transformer
val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIdx")

// transform 后 dataframe 多了 categoryIdx 列
val transformed = indexer.fit(df).transform(df)
```

Transformer 的特点总结：

| 特性 | 说明 |
|------|------|
| 核心方法 | `transform(dataset: Dataset[_]): DataFrame` |
| 输入 → 输出 | DataFrame → DataFrame |
| 作用 | 增加新列、转换已有列 |
| 需要 fit 吗？ | 有的需要（如 StringIndexer 要先统计数据频率），有的不需要（如 VectorAssembler 就是简单的列合并） |
| 典型例子 | StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, PCA, Tokenizer |

### Estimator（估计器）

Estimator 是 Pipeline 的"老师"，它的职责是从数据中 **学习** 模型：

```scala
import org.apache.spark.ml.Estimator

// 常见的 Estimator
val rf = new RandomForestClassifier()
  .setFeaturesCol("features")
  .setLabelCol("label")

// fit() 返回 RandomForestClassificationModel（也是一个 Transformer）
val model = rf.fit(trainDF)
// model 可以对新数据进行 transform() 来预测
```

Estimator 的特点总结：

| 特性 | 说明 |
|------|------|
| 核心方法 | `fit(dataset: Dataset[_]): Model` |
| 输入 → 输出 | DataFrame → Transformer (Model) |
| 作用 | 从数据中学习参数，返回一个可以用于预测的 Model |
| 典型例子 | LogisticRegression, RandomForestClassifier, KMeans, ALS, GBTClassifier |

> **面试点**：Pipeline 的集合规则——Pipeline 中可以有**任意多个 Transformer + 最后一个 Estimator**。如果全是 Transformer，只能做特征处理，不能训练模型。如果最后一个不是 Estimator 而是 Transformer，Pipeline 的 `fit()` 会报错。

### Pipeline 组装

把上面的组件串起来，就是完整的 Pipeline：

```scala
import org.apache.spark.ml.{Pipeline, PipelineModel}

val pipeline = new Pipeline()
  .setStages(Array(indexer, encoder, assembler, scaler, rf))

// 训练——整条流水线运行
val pipelineModel: PipelineModel = pipeline.fit(trainDF)

// 预测——只需一行代码
val predictions = pipelineModel.transform(testDF)

// 查看预测结果
predictions.select("id", "features", "prediction", "probability").show()
```

### Pipeline 执行过程的详细拆解

Pipeline 的 `fit()` 执行流程是怎样的？下面以 `StringIndexer → VectorAssembler → RandomForestClassifier` 为例：

1. **Stage 0: StringIndexer（Transformer）**
   - `fit(trainDF)`：扫描 `category` 列，统计每个类别的出现频率，构建 {类别 -> 索引} 的映射表
   - `transform(trainDF)`：给训练数据增加 `categoryIdx` 列

2. **Stage 1: VectorAssembler（Transformer）**
   - `transform(trainDF)`：将 `age`, `income`, `categoryIdx` 三列合并为 `features` 列（VectorAssembler 不需要 fit，它不学习任何参数）

3. **Stage 2: RandomForestClassifier（Estimator）**
   - `fit(trainDF)`：使用 `features` 列和 `label` 列训练随机森林模型
   - 返回 `RandomForestClassificationModel`

4. 最终返回的 `PipelineModel` 内部持有：
   - Stage 0: 已 fitted 的 StringIndexerModel（知道每种城市对应什么索引）
   - Stage 1: VectorAssembler（无状态）
   - Stage 2: RandomForestClassificationModel（树的结构和分裂点）

当你调用 `pipelineModel.transform(newData)` 时：
- 自动按顺序执行每个 Stage 的 `transform()`方法
- 不需要再调用 feature()——PipelineModel 自动帮你做所有特征工程

## Pipeline 参数管理

### ParamGrid 与网格搜索

模型训练的一个核心问题是如何找到最优的超参数。MLlib 提供了 ParamGridBuilder 来枚举超参数组合，配合 CrossValidator 进行自动调参：

```scala
import org.apache.spark.ml.tuning.{ParamGridBuilder, CrossValidator}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator

// 定义参数网格——所有组合的笛卡尔积
val paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(20, 50, 100))         // 3 种选择
  .addGrid(rf.maxDepth, Array(5, 10, 15))           // 3 种选择
  .addGrid(rf.impurity, Array("gini", "entropy"))   // 2 种选择
  .build()  // 一共 3 × 3 × 2 = 18 种组合

// 评估器
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("f1")

// 交叉验证
val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)
  .setParallelism(3)  // 并行训练的模型数

val cvModel = cv.fit(trainDF)

// 获取最佳模型
println(cvModel.bestModel.asInstanceOf[PipelineModel]
  .stages.last.asInstanceOf[RandomForestClassificationModel]
  .explainParams())
```

### CrossValidator 的计算量估算

CrossValidator + ParamGrid 的计算量是 **ParamGrid大小 × K折**。上面的例子中：

- 18 种参数组合 × 3 折 = 54 个模型
- 如果每个模型训练 5 分钟，总共需要 270 分钟（4.5 小时）

所以在实际项目中，网格搜索需要控制参数搜索范围，或者先在大范围上粗搜，再在小范围上精搜。

> **踩坑经验**：`setParallelism` 控制的是 CrossValidator 的并行度，但要注意不是越大越好。并行度受数据集大小和集群资源的限制。如果设置过大，每个 Executor 分到的数据量太小，反而因为调度开销导致更慢。推荐的并行度是 `min(paramGridSize, executorCoresCount)`。

### TrainValidationSplit（更快但不如 CV 稳定）

当数据量非常大（几百 GB 以上），做 K 折交叉验证的成本太高时，可以用 TrainValidationSplit 替代：

```scala
import org.apache.spark.ml.tuning.TrainValidationSplit

val tvs = new TrainValidationSplit()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setTrainRatio(0.8)  // 80% 训练，20% 验证
  .setParallelism(3)
```

CrossValidator vs TrainValidationSplit 该怎么选？

| 对比维度 | CrossValidator | TrainValidationSplit |
|---------|---------------|---------------------|
| 验证方式 | K 折交叉验证 | 单次训练/验证划分 |
| 训练次数 | K × paramGridSize | 1 × paramGridSize |
| 评估稳定性 | 高（K 次评估取平均） | 低（取决于单次划分） |
| 小数据集 | 推荐（充分利用数据） | 不推荐（浪费 20% 验证数据） |
| 大数据集 | 代价高 | 推荐 |
| 偏差 | 低偏差 | 高偏差（验证集可能不具代表性） |

## Pipeline 持久化

训练好的 PipelineModel 可以完整保存到磁盘或 HDFS，包含所有 Stage 的参数和状态：

```scala
// 保存 Pipeline 模型
val modelPath = "hdfs://models/rf_pipeline"
pipelineModel.write.overwrite().save(modelPath)

// 加载 Pipeline 模型
val loadedModel = PipelineModel.load(modelPath)

// 对新的数据做预测——不需要再手动做特征工程！
val newPredictions = loadedModel.transform(newData)
```

持久化的目录结构是什么样的？

```
hdfs://models/rf_pipeline/
├── _SUCCESS
├── metadata/
│   ├── part-00000    # Pipeline 元数据
│   └── ...
├── stages/
│   ├── 0_StringIndexer_xxx/
│   │   ├── data/           # 类别到索引的映射表
│   │   └── metadata/
│   ├── 1_VectorAssembler_xxx/
│   │   └── metadata/       # 输入输出列配置
│   ├── 2_StandardScaler_xxx/
│   │   ├── data/           # 均值和标准差
│   │   └── metadata/
│   └── 3_RFClassifier_xxx/
│       ├── data/           # 树的结构和分裂点
│       └── metadata/
└── ...
```

模型版本管理的最佳实践：

```scala
// 用版本目录管理——支持回滚
// hdfs://models/
//   ├── v1_20240101/   # 初始版本
//   ├── v2_20240201/   # 优化了特征的版本
//   └── v3_20240301/   # 当前线上版本

// 加载时使用符号链接或配置管理
val modelVersion = sys.env.getOrElse("MODEL_VERSION", "v3")
val modelPath = s"hdfs://models/${modelVersion}/rf_pipeline"
val model = PipelineModel.load(modelPath)
```

> **面试点**：Pipeline 持久化为什么是面试高频考点？因为它解决了**训练-预测特征不一致（Training-Serving Skew）** 这个工业界的大难题。很多公司在模型上线时踩过坑：开发环境用手动写好的特征工程代码，线上部署的时候遗漏了某一步或者写错了参数，导致模型效果大幅下降。Pipeline 把整个流程打包在一起，从源头上杜绝了这个问题。

## 自定义 Pipeline Stage

当内置的 Transformer 不能满足业务需求时，可以自己写一个自定义的 Transformer：

```scala
import org.apache.spark.ml.{Transformer, UnaryTransformer}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset}

// 自定义 Transformer：清理文本（去特殊字符 + 转小写）
class TextCleaner(override val uid: String)
  extends Transformer with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("textCleaner"))

  // 定义参数
  val inputCol = new Param[String](this, "inputCol", "input column name")
  val outputCol = new Param[String](this, "outputCol", "output column name")

  def setInputCol(value: String): this.type = set(inputCol, value)
  def setOutputCol(value: String): this.type = set(outputCol, value)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val transformUDF = dataset.sqlContext.udf.register(
      s"${uid}_clean",
      (s: String) => s.toLowerCase.replaceAll("[^a-zA-Z\\s]", "")
    )
    dataset.withColumn($(outputCol), transformUDF(dataset($(inputCol))))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    require(schema.fieldNames.contains($(inputCol)))
    schema.add(StructField($(outputCol), StringType, nullable = true))
  }
}

// 使用自定义 Transformer
val pipeline = new Pipeline()
  .setStages(Array(
    new TextCleaner().setInputCol("raw_text").setOutputCol("clean_text"),
    new HashingTF().setInputCol("clean_text").setOutputCol("features"),
    new LogisticRegression()
  ))
```

### 自定义 Pipeline Stage 的最佳实践

| 准则 | 说明 |
|------|------|
| 继承 `DefaultParamsWritable` | 确保自定义 Stage 可以被序列化保存 |
| 实现 `transformSchema` | 让 Pipeline 在运行前就能做 Schema 校验 |
| 使用 `Identifiable.randomUID` | 生成唯一 ID，避免多个同类型 Stage 冲突 |
| 万不得已再自定义 | 先确认内置 Transformer 能否满足需求 |

## 面试高频考点

### Q: Pipeline 中 Transformer 和 Estimator 的区别？

Transformer 实现 `transform()` 方法，输入 DataFrame 输出 DataFrame（通常会增加新列）。Estimator 实现 `fit()` 方法，输入 DataFrame 输出 Transformer（Model）。Pipeline 中可以有多个 Transformer 但只能有一个 Estimator 放在最后。

### Q: CrossValidator 和 TrainValidationSplit 的区别？

CrossValidator 做 K 折交叉验证（默认 3 折），每折的训练/验证数据按比例切分，能更稳定地评估模型效果，但需要训练 K 倍次数。TrainValidationSplit 只做一次训练/验证划分（默认 80/20），速度更快，但对数据划分方式敏感，评估结果方差更大。

> 选择建议：数据量小于 100GB 用 CrossValidator，大于 100GB 用 TrainValidationSplit。

### Q: Pipeline 如何保存和加载？

Pipeline 和 PipelineModel 都支持 `write.save(path)` 和 `PipelineModel.load(path)`。保存的是 Spark 的 ML 格式（Parquet 文件 + 元数据），包含所有 Stage 的参数。加载后直接对原始数据 `transform()` 即可，不需要手动做特征处理。

### Q: 为什么说 Pipeline 能解决训练预测特征不一致问题？

因为 PipelineModel 将所有的特征转换逻辑（StringIndexer 的映射表、StandardScaler 的均值和标准差等）打包保存在模型文件中。加载模型进行预测时，会自动应用完全相同的转换逻辑，不需要手动编写特征工程代码，彻底避免了人为失误。

## 小结

| 概念 | 要点 | 一句话总结 |
|------|------|-----------|
| Pipeline | 按顺序执行的 Stage 集合 | 把多个算法步骤串成一条流水线 |
| Transformer | `transform()` 做转换 | 输入 DF 输出 DF，增加新列 |
| Estimator | `fit()` 训练模型，返回 Model | 输入 DF 输出一个可以预测的 Model |
| ParamGrid | 超参数组合的笛卡尔积 | 枚举所有想尝试的参数组合 |
| CrossValidator | K 折交叉验证，选择最佳参数 | 评估稳定但慢 |
| TrainValidationSplit | 一次训练/验证划分 | 快但评估不稳定 |
| 持久化 | write.save / PipelineModel.load | 完整保存所有 Stage 状态 |
