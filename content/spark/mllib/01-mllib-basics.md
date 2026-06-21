# Spark MLlib — 分布式机器学习基础

## Spark MLlib 概览

### 为什么需要 MLlib？

在 Spark 出现之前，大数据场景下做机器学习是一件非常痛苦的事情。传统做法是：先用 MapReduce/Hive 做数据清洗和特征工程，然后把处理好的数据导出到单机 Python/R 环境中训练模型。这条链路存在几个致命的痛点：

| 痛点 | 描述 | 后果 |
|------|------|------|
| 数据搬运 | 需要在 Hadoop 和 Python 之间反复拷贝数据 | I/O 开销巨大，耗时成倍增加 |
| 内存限制 | 单机内存有限，无法处理 TB 级数据 | 必须采样，降低模型精度 |
| 流程割裂 | 数据处理和模型训练各玩各的 | 上线时特征逻辑不一致，导致效果衰减 |
| 迭代缓慢 | 修改特征后全量重跑流程 | 模型迭代周期以天/周为单位 |

**MLlib 的诞生彻底解决了这些问题**——它让数据处理和模型训练跑在同一个分布式引擎上，不再需要数据搬运，而且 Pipeline 机制保证了从训练到上线的特征逻辑完全一致。

### MLlib 两大 API 包的演化

MLlib 在发展过程中经历了重大的 API 重构，了解其演进历史对面试非常有帮助：

```
MLlib 两大 API 包：
├── spark.mllib  (RDD API，Spark 1.x ~ 2.x 主力，现已进入维护模式)
│   ├── 基于 RDD 的数据结构
│   ├── API 不够友好，需要手动管理特征列
│   └── 官方已不在积极开发新功能
│
└── spark.ml     (DataFrame API，Spark 2.x 至今的主力)
    ├── 基于 DataFrame 的 API，和 SQL 无缝集成
    ├── 支持 Pipeline 机制，可以组合多个算法步骤
    └── 所有新算法只会加在这里，旧的 spark.mllib 不再更新
```

> **面试点**：被问到 MLlib 时，务必优先讨论 spark.ml 的 DataFrame API + Pipeline。在 Spark 3.x 中，spark.mllib 包已经处于维护模式，spark.ml 才是官方推荐的主力 API。如果面试官问到"spark.mllib 和 spark.ml 的区别"，这就是标准答案。

### MLlib 能做什么？

MLlib 覆盖了机器学习的主要领域，但要注意它的定位——它不是万能的，在某些场景下有明显的局限性：

| 领域 | MLlib 支持情况 | 代表性算法 | 适用场景 |
|------|---------------|-----------|---------|
| 分类与回归 | 完善 | LR, RF, GBDT, NB, LinearRegression | 大部分经典 ML 任务 |
| 聚类 | 完善 | K-Means, Bisecting K-Means, GMM, LDA | 用户分群、文本主题 |
| 推荐 | 完善 | ALS（协同过滤） | 电商/视频推荐 |
| 特征工程 | 完善 | StringIndexer, OneHotEncoder, Scaler, PCA | 全链路特征处理 |
| 频繁模式挖掘 | 支持 | FP-Growth, PrefixSpan | 购物篮分析、序列挖掘 |
| 深度学习 | ❌ 不支持 | — | 需要 TensorFlow/PyTorch |
| 在线学习 | ❌ 不支持 | — | 需要实时增量更新 |

### MLlib 与传统单机 ML 框架的对比

| 维度 | Spark MLlib | scikit-learn | TensorFlow/PyTorch |
|------|------------|-------------|-------------------|
| 数据处理能力 | TB ~ PB 级 | GB 级（受单机内存限制） | GB 级 |
| 训练速度 | 分布式并行 | 单机（只能靠 CPU 单核） | GPU 加速 |
| 算法丰富度 | 经典 ML 算法 | 经典 ML 算法很丰富 | 深度学习 |
| API 易用性 | 中（需要 Scala/Java/Python） | 高（Python 生态） | 中 |
| 部署复杂度 | 需要 Spark 集群 | pip install 即可 | 需要 GPU 环境 |
| 适合场景 | 大数据 ETL + ML 全流程 | 快速原型、小数据 | CV/NLP 等深度任务 |

> **实际经验**：在工业界生产环境中，最常见的架构是 "Spark 做特征工程和批处理 -> TensorFlow/PyTorch 做模型训练" 的组合模式。MLlib 更适合不需要深度学习的场景，或者特征工程极复杂的场景——因为 Spark 的分布式能力让 TB 级特征处理变得轻而易举。

## ML Pipeline

### Pipeline 设计理念

Spark ML Pipeline 的设计借鉴了 scikit-learn 的 Pipeline 概念，将**数据处理**和**模型训练**组织为一个有向无环图（DAG）。但和 scikit-learn 的一个关键区别是：Spark 的 Pipeline 是分布式的，每个 Stage 都在集群上并行执行。

为什么要用 Pipeline？其实我们完全可以把每个步骤分开写，但 Pipeline 带来了三个核心价值：

1. **代码组织**：所有步骤在一个 Pipeline 中定义，逻辑清晰，便于维护
2. **避免特征泄露**：每个 Stage 在 `fit()` 时只看到训练数据，`transform()` 时用同样的逻辑处理测试数据
3. **一键部署**：训练好的 PipelineModel 可以用一个 `transform()` 完成从原始数据到预测结果的整个过程

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

### 完整的 Pipeline 示例

```scala
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.RandomForestClassifier

// Step 1: 特征转换 — 将字符串类别转为数值索引
val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIdx")

// Step 2: 向量组装 — 将所有特征合并为一个向量列
val assembler = new VectorAssembler()
  .setInputCols(Array("age", "income", "categoryIdx"))
  .setOutputCol("features")

// Step 3: 特征缩放 — 标准化到相同量纲
val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")

// Step 4: 模型 — 随机森林分类器
val rf = new RandomForestClassifier()
  .setFeaturesCol("scaledFeatures")
  .setLabelCol("label")

// 组装 Pipeline
val pipeline = new Pipeline()
  .setStages(Array(indexer, assembler, scaler, rf))

// 训练 — 注意：这里传入的是原始数据
// Pipeline 会自动按顺序执行 fit() 和 transform()
val model = pipeline.fit(trainDF)
val predictions = model.transform(testDF)
```

### Pipeline 的 Stage 类型

| Stage 类型 | 方法 | 输入 → 输出 | 示例 |
|-----------|------|------------|------|
| **Transformer** | `transform()` | DataFrame → DataFrame（增加列） | StringIndexer, VectorAssembler, StandardScaler |
| **Estimator** | `fit()` | DataFrame → Model（Transformer） | RandomForestClassifier, KMeans, ALS |

> **踩坑经验**：Pipeline 中 Estimator 只能出现在最后一个 Stage！如果在 Estimator 后面还有 Transformer，Pipeline 会报错。这是因为 Pipeline 必须在执行完所有转换之后，才能把数据喂给模型做训练。

### Pipeline 的执行流程详解

Pipeline 的 `fit()` 执行流程是这样的：

1. Stage 0 (Transformer, e.g., StringIndexer)：先 `fit()` 统计数据分布，再 `transform()` 转换数据
2. Stage 1 (Transformer, e.g., VectorAssembler)：同样的 `fit()` → `transform()`
3. Stage 2 (Transformer, e.g., StandardScaler)：`fit()` 计算均值和标准差，`transform()` 标准化
4. Stage 3 (Estimator, e.g., RandomForestClassifier)：`fit()` 训练模型，返回 Model（也是一个 Transformer）

最终返回的 PipelineModel 内部包含所有 Stage 的 fitted 版本。当你调用 `model.transform(testDF)` 时，自动按顺序执行每个 Stage 的 `transform()`。

### Pipeline 的持久化

```scala
// 保存完整的 PipelineModel
model.write.overwrite().save("hdfs://models/rf_model")

// 加载已训练的 PipelineModel
val loadedModel = PipelineModel.load("hdfs://models/rf_model")

// 加载后直接对新的原始数据进行预测
val newPredictions = loadedModel.transform(newData)
```

PipelineModel 的持久化保存的是**完整的处理链路**——包括 StringIndexer 的词典、StandardScaler 的均值和标准差、以及训练好的 Random Forest 模型。这就意味着：

- 加载模型后，直接给原始数据就行，不需要手动重复特征工程步骤
- 训练和预测的特征逻辑 100% 一致，不会出现线上线下的特征不一致问题

> **面试点**：Pipeline 持久化为啥重要？因为模型上线的最大坑就是"训练-预测特征不一致"（Training-Serving Skew）。Pipeline 把特征工程和模型打包在一起，从根本上解决了这个问题。这是面试中常考的知识点。

## 特征工程

### 为什么特征工程是 ML Pipeline 中最重要的一环？

有句话在机器学习领域广为流传："**数据和特征决定了机器学习的上限，而模型和算法只是逼近这个上限。**"在实际工业场景中，花在特征工程上的时间往往占整个项目周期的 60% ~ 80%。

原因很简单：**原始数据很少能直接喂给模型**。你的数据里可能有中文城市名、缺失值、不同量纲的数值特征，这些都需要转换为模型能够理解的数值向量。

### StringIndexer — 字符串索引化

StringIndexer 是最常用的特征转换器之一，它会把字符串类别的值转换为数值索引：

```scala
import org.apache.spark.ml.feature.StringIndexer

val indexer = new StringIndexer()
  .setInputCol("city")
  .setOutputCol("cityIdx")
  .setHandleInvalid("keep")  // 处理未知值：keep/skip/error

val indexed = indexer.fit(df).transform(df)
// "北京" → 0.0, "上海" → 1.0, "广州" → 2.0（按频率降序排列）
```

关于 StringIndexer，有几个容易踩坑的点：

| 问题 | 说明 | 解决方案 |
|------|------|---------|
| 测试集出现未知类别 | 训练集有"北京""上海"，测试集出现"深圳" | `setHandleInvalid("keep")` 保留未知值 |
| 索引值带顺序含义 | 北京=0, 上海=1, 广州=2 隐含了大小关系 | 配合 OneHotEncoder 使用 |
| 频率排序不稳定 | 数据量小时，顺序可能变化 | 设置固定 seed 或手动指定标签顺序 |

> **踩坑经验**：`setHandleInvalid("keep")` 是生产环境中的必选项。默认行为是 `"error"`——这意味着如果预测数据中有一个训练集没见过的城市名，任务直接报错挂掉。在线上环境中，未知值是常态，用 `"keep"` 可以让模型优雅地处理这些情况。

### OneHotEncoder — 独热编码

StringIndexer 将城市名转为 0/1/2，但这里有个隐藏问题：数字 2 天然被认为比 0 大，模型会错误地认为"广州 > 北京"。OneHotEncoder 解决了这个问题：

```scala
import org.apache.spark.ml.feature.OneHotEncoder

val encoder = new OneHotEncoder()
  .setInputCol("cityIdx")
  .setOutputCol("cityVec")
  .setDropLast(true)  // 丢弃最后一个类别，防止多重共线性
// 0.0 → [1.0, 0.0]
// 1.0 → [0.0, 1.0]
// 2.0 → [0.0, 0.0]（被丢弃）
```

为什么 `setDropLast(true)`？对于 K 个类别，K-1 个二元特征就足够了。丢弃最后一个可以避免线性回归中的多重共线性问题。但如果你用的是决策树这类树模型，可以设置 `dropLast=false`，因为树模型不受多重共线性影响。

### Bucketizer — 连续值离散化

有时候我们想把连续的数值特征（如年龄）变成离散的分组特征（如未成年、青年、中年、老年）：

```scala
import org.apache.spark.ml.feature.Bucketizer

// 定义分桶边界（包含边界数 = 分段数 + 1）
val splits = Array(Double.NegativeInfinity, 0, 18, 35, 60, Double.PositiveInfinity)

val bucketizer = new Bucketizer()
  .setInputCol("age")
  .setOutputCol("ageGroup")
  .setSplits(splits)
// 年龄 → 分组：0-18=0, 18-35=1, 35-60=2, 60+=3
```

| 数据预处理方法 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| 连续值直接输入 | 线性关系强（如收入对消费的影响） | 信息损失最小 | 对异常值敏感 |
| Bucketizer 分桶 | 非线性关系（如年龄对患病率） | 捕获非线性，对异常值鲁棒 | 信息有损失 |
| 分位数离散化 | 长尾分布的特征 | 每个桶样本量接近 | 边界可能不自然 |

### VectorAssembler — 特征向量组装

**这是整个特征工程中最常用的 Transforme**——它把多列合并为一个特征向量，因为 MLlib 的所有算法都要求输入是一个 Vector 类型的列：

```scala
import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()
  .setInputCols(Array("age", "income", "cityVec", "educationIdx"))
  .setOutputCol("features")
// 将多个数值列合并为一个特征向量
// 注意：所有列必须是数值类型（Double, Long, Int）或 Vector 类型
```

> **踩坑经验**：`VectorAssembler` 的 `inputCols` 中如果混入了 String 类型的列，会直接报错。所以在做 Assembler 之前，确保所有列都是数值类型。比较常见的做法是：先 `StringIndexer` 再 `OneHotEncoder`，确保所有列都转为数值后再进行 `VectorAssembler`。

### StandardScaler — 标准化

为什么要标准化？想象一下：年龄 0~100 岁，收入 0~100 万，这两个特征的量纲差了 10000 倍。如果不做标准化，大部分模型会优先关注收入这个特征，因为它的数值变化范围大，导致的结果就是——**年龄这个特征几乎被模型忽略了**。

```scala
import org.apache.spark.ml.feature.StandardScaler

val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)   // 除以标准差，使方差为 1（默认 true）
  .setWithMean(true)  // 减去均值，使数据居中（只支持稠密向量，默认 false）
```

哪些模型需要标准化，哪些不需要？

| 模型类型 | 需要标准化吗？ | 原因 |
|---------|--------------|------|
| 线性回归、逻辑回归 | **必须** | 梯度下降需要各维度尺度一致 |
| SVM | **必须** | 对特征尺度敏感 |
| K-Means、KNN | **必须** | 基于距离计算，量纲会主导结果 |
| PCA | **必须** | 方差越大的维度权重越大 |
| 决策树、随机森林 | **不需要** | 基于阈值分裂，不受特征尺度影响 |
| 朴素贝叶斯 | **视情况** | 高斯分布假设下需要，多伯努力不需要 |

> **踩坑经验**：`setWithMean(true)` 对稀疏数据是危险的！稀疏数据中大部分元素是 0，减去均值后变成负数，破坏了稀疏性——原本的 0 不再为 0，内存占用和计算量都暴增。所以对于稀疏高维特征（如文本 TF-IDF 特征），只使用 `setWithStd(true)` 即可。

### MinMaxScaler — 归一化

```scala
import org.apache.spark.ml.feature.MinMaxScaler

val minMax = new MinMaxScaler()
  .setInputCol("features")
  .setOutputCol("normalizedFeatures")
  .setMin(0.0)
  .setMax(1.0)
// 缩放到 [0, 1] 区间
// (x - min) / (max - min) × (max - min) + min
```

StandardScaler 和 MinMaxScaler 的选择：

| 对比维度 | StandardScaler | MinMaxScaler |
|---------|---------------|-------------|
| 输出范围 | 均值 0，标准差 1 | 固定 [0, 1] 或 [-1, 1] |
| 对异常值的鲁棒性 | 较强（用标准差） | 较弱（被 min/max 拉偏） |
| 稀疏数据兼容性 | 可以（只设 withStd） | 可以 |
| 适用算法 | SVM、LR、神经网络 | KNN、神经网络（像素值） |

### MaxAbsScaler — 绝对值归一化

```scala
import org.apache.spark.ml.feature.MaxAbsScaler

val maxAbs = new MaxAbsScaler()
  .setInputCol("features")
  .setOutputCol("maxAbsFeatures")
// 缩放到 [-1, 1]，保留稀疏性（0 映射后仍是 0）
```

## 分类与回归

### 逻辑回归

逻辑回归虽然叫"回归"，但实际上最常用的分类算法。它的输出是样本属于每个类别的概率：

```scala
import org.apache.spark.ml.classification.LogisticRegression

val lr = new LogisticRegression()
  .setMaxIter(100)
  .setRegParam(0.01)     // L2 正则化系数（越大越防过拟合）
  .setElasticNetParam(0.0)  // 0=L2 正则, 1=L1 正则
  .setFamily("multinomial") // auto/binomial(二分类)/multinomial(多分类)

val model = lr.fit(trainDF)

// 查看模型参数
println(s"Coefficients: ${model.coefficientMatrix}")
println(s"Intercept: ${model.interceptVector}")

// 训练集上的评估指标
println(s"Training accuracy: ${model.summary.accuracy}")
```

> **面试点**：逻辑回归在工业界非常受欢迎，主要原因是**可解释性强**——你可以直接说出"收入每增加 1 万，违约概率增加 0.3%"。在金融风控、医疗诊断等对可解释性要求高的场景，逻辑回归是首选。

### 随机森林

随机森林是一种 Bagging 集成方法，它训练多棵决策树，每棵树在训练数据的随机子集和随机特征子集上学习：

```scala
import org.apache.spark.ml.classification.RandomForestClassifier

val rf = new RandomForestClassifier()
  .setNumTrees(100)      // 树的数量（越多越稳定，但计算量线性增长）
  .setMaxDepth(10)       // 最大深度（太深容易过拟合）
  .setMaxBins(32)        // 连续特征离散化的桶数
  .setFeatureSubsetStrategy("sqrt")  // 每棵树使用的特征数策略
  .setImpurity("gini")   // 分裂标准：gini / entropy
  .setSubsamplingRate(1.0)  // 每棵树的样本采样比例

val model = rf.fit(trainDF)
println(s"Feature Importances: ${model.featureImportances}")
// 每个特征的重要性排名 —— 这个信息非常有用！
// 可以指导你做特征筛选：值太小的特征可以考虑删除
```

随机森林的几个关键参数调优经验：

| 参数 | 调优建议 | 说明 |
|------|---------|------|
| `numTrees` | 100 ~ 500，越大越好但收益递减 | 500 棵以后效果提升很小 |
| `maxDepth` | 10 ~ 20 | 太浅欠拟合，太深过拟合 |
| `minInstancesPerNode` | 1% ~ 5% 的样本量 | 限制叶子节点最小样本数 |
| `featureSubsetStrategy` | 分类用 sqrt，回归用 n/3 | 经验公式 |

### GBDT (Gradient Boosted Trees)

GBDT 用 Boosting 的思路，每棵新树去拟合前一棵树的残差：

```scala
import org.apache.spark.ml.classification.GBTClassifier

val gbt = new GBTClassifier()
  .setMaxIter(50)        // 树的数量（越大越可能过拟合）
  .setMaxDepth(5)        // GBDT 的树通常较浅（3~8 层）
  .setStepSize(0.1)      // 学习率（小步长 + 多迭代 = 更好效果）
  
val model = gbt.fit(trainDF)
```

随机森林 vs GBDT 的选择：

| 对比维度 | 随机森林 | GBDT |
|---------|---------|------|
| 训练方式 | Bagging（并行） | Boosting（串行） |
| 抗过拟合 | 强 | 弱（容易过拟合） |
| 对异常值 | 鲁棒 | 敏感 |
| 调参难度 | 容易 | 较难（学习率 + 树数量需要配合调整） |
| 适合场景 | 高维稀疏数据 | 低维稠密数据 |
| 特征重要性 | 直接提供 | 直接提供 |

## 聚类

### K-Means

K-Means 是最常用的聚类算法，目标是将数据划分为 K 个簇，使簇内的样本尽可能相似：

```scala
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

val kmeans = new KMeans()
  .setK(5)              // 聚类数（最重要的参数！）
  .setSeed(42)          // 随机种子（保证可复现）
  .setMaxIter(50)       // 最大迭代次数
  .setInitMode("k-means||")  // 初始化方式：k-means|| / random
  .setInitSteps(2)      // K-Means|| 的步数

val model = kmeans.fit(featuresDF)

// 查看聚类中心
println(s"Cluster Centers: ${model.clusterCenters.mkString(", ")}")

// 预测每个样本所属的簇
val predictions = model.transform(featuresDF)

// 评估 — 轮廓系数
val evaluator = new ClusteringEvaluator()
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
  .setMetricName("silhouette")  // 轮廓系数

val silhouette = evaluator.evaluate(predictions)
// 范围 [-1, 1]  > 0.5 表示有合理的聚类结构
```

如何选择最优的 K？常用的是肘部法则：

```scala
// 遍历 K 值，画出 SSE 曲线，找到拐点
(2 to 10).map { k =>
  val km = new KMeans().setK(k).setSeed(42).setFeaturesCol("features")
  val m = km.fit(featuresDF)
  val sil = evaluator.evaluate(m.transform(featuresDF))
  (k, m.summary.trainingCost, sil)
}.foreach { case (k, cost, sil) =>
  println(s"K=$k, SSE=$cost, Silhouette=$sil")
}
// 拐点：SSE 下降变缓的位置，就是最优 K 值
```

K-Means 的局限性：

| 局限 | 说明 | 应对方案 |
|------|------|---------|
| 必须预先指定 K | 实际场景 K 通常未知 | 肘部法 + 业务判断 |
| 对初始中心敏感 | 不同 seed 结果不同 | 设置固定 seed，多跑几次取稳定结果 |
| 只能发现球形簇 | 无法处理复杂形状 | 试试 DBSCAN 或 GMM |
| 对异常值敏感 | 离群点会拉偏簇中心 | 先做异常检测和过滤 |
| 高维数据效果差 | 维度灾难，距离度量失效 | 先用 PCA 降维 |

## 推荐算法 (ALS)

ALS（交替最小二乘法）是 Spark 最成熟的协同过滤算法，非常适合做推荐系统：

```scala
import org.apache.spark.ml.recommendation.ALS

val als = new ALS()
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")
  .setRank(10)          // 隐因子维度（越大越精确但越慢）
  .setMaxIter(20)       // 最大迭代次数
  .setRegParam(0.05)    // 正则化系数
  .setAlpha(1.0)        // 隐式反馈的置信度参数
  .setImplicitPrefs(false)  // false=显式评分(1-5)，true=隐式反馈(点击/购买)

val model = als.fit(trainDF)

// 为每个用户推荐 10 个物品
val userRecs = model.recommendForAllUsers(10)
// 为物品推荐可能感兴趣的用户
val itemRecs = model.recommendForAllItems(10)

// 为指定的少量用户推荐
val targetedRecs = model.recommendForUserSubset(usersDF, 10)
```

> **面试点**：ALS 为什么适合分布式？因为 ALS 交替固定用户矩阵或物品矩阵时，每个用户/物品的计算是独立的——天然适合并行化。这也是 ALS 在 Spark 中如此流行的原因。

## 模型评估

MLlib 提供了多种评估器，不同类型的任务使用不同的评估指标：

```scala
// 分类评估
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")  // 可选: f1, weightedPrecision, weightedRecall

val accuracy = evaluator.evaluate(predictions)

// 回归评估
val regEval = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse")  // 可选: mse, r2, mae

val rmse = regEval.evaluate(predictions)

// 交叉验证 + 网格搜索
val paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(50, 100, 200))
  .addGrid(rf.maxDepth, Array(5, 10, 15))
  .build()

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)

val cvModel = cv.fit(trainDF)
// cvModel.bestModel 就是最优参数组合训练出来的模型
```

## 小结

| 组件 | 用途 | 面试高频问题 |
|------|------|------------|
| Pipeline | 统一工作流，包含 Transformer + Estimator | Pipeline 的执行顺序？每次 fit() 和 transform() 的流程？ |
| 特征工程 | StringIndexer → OneHotEncoder → VectorAssembler → Scaler | 为什么需要特征标准化？稀疏数据能否减均值？ |
| 分类 | LogisticRegression, RandomForest, GBT, NaiveBayes | RF 和 GBDT 的区别？L1 和 L2 的区别？ |
| 聚类 | KMeans, BisectingKMeans, LDA, GaussianMixture | K 值怎么选？K-Means 的优缺点？ |
| 推荐 | ALS | ALS 为什么适合分布式？显式和隐式反馈的区别？ |
| 评估 | CrossValidator, TrainValidationSplit | CV 的 K 值怎么选？数据划分的注意点？ |
