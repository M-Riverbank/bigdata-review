# Spark MLlib — 分布式机器学习基础

## Spark MLlib 概览

MLlib 是 Spark 的分布式机器学习库，支持分类、回归、聚类、推荐、特征工程等。

```
MLlib 两大 API 包：
├── spark.mllib  (旧 RDD API，维护模式)
└── spark.ml     (新 DataFrame API，当前主力)
```

> **面试提示**：被问到 MLlib 时，优先讨论 spark.ml 的 DataFrame API + Pipeline。

## ML Pipeline

Pipeline 将数据处理、特征工程、模型训练整合为一个可重复的工作流。

```scala
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.RandomForestClassifier

// Step 1: 特征转换
val indexer = new StringIndexer()
  .setInputCol("category")
  .setOutputCol("categoryIdx")

// Step 2: 向量组装
val assembler = new VectorAssembler()
  .setInputCols(Array("age", "income", "categoryIdx"))
  .setOutputCol("features")

// Step 3: 特征缩放
val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")

// Step 4: 模型
val rf = new RandomForestClassifier()
  .setFeaturesCol("scaledFeatures")
  .setLabelCol("label")

// 组装 Pipeline
val pipeline = new Pipeline()
  .setStages(Array(indexer, assembler, scaler, rf))

// 训练
val model = pipeline.fit(trainDF)
val predictions = model.transform(testDF)
```

### Pipeline 的 Stage 类型

| Stage 类型 | 说明 | 示例 |
|-----------|------|------|
| **Transformer** | 输入 DF → 输出 DF | StringIndexer, VectorAssembler |
| **Estimator** | 输入 DF → 输出 Model(TF) | RandomForestClassifier, KMeans |

### Pipeline 的持久化

```scala
model.write.overwrite().save("hdfs://models/rf_model")
val loadedModel = PipelineModel.load("hdfs://models/rf_model")
```

## 特征工程

### StringIndexer — 字符串索引化

```scala
val indexer = new StringIndexer()
  .setInputCol("city")
  .setOutputCol("cityIdx")
  .setHandleInvalid("keep")  // 处理未知值：keep/skip/error
// "北京" → 0, "上海" → 1, "广州" → 2（按频率排序）
```

### OneHotEncoder — 独热编码

```scala
val encoder = new OneHotEncoder()
  .setInputCol("cityIdx")
  .setOutputCol("cityVec")
// 0 → [1, 0, 0]
// 1 → [0, 1, 0]
// 2 → [0, 0, 1]
```

### VectorAssembler — 向量组装

```scala
val assembler = new VectorAssembler()
  .setInputCols(Array("feature1", "feature2", "categoryVec"))
  .setOutputCol("features")
// [1.0, 2.5, 0, 1, 0] → features 向量
```

### StandardScaler — 标准化

```scala
val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)   // 标准差标准化
  .setWithMean(true)  // 减去均值（稠密向量才支持）
```

## 分类与回归

### 逻辑回归

```scala
val lr = new LogisticRegression()
  .setMaxIter(100)
  .setRegParam(0.01)     // L2 正则系数
  .setElasticNetParam(0.0)  // 0=L2, 1=L1
  .setFamily("multinomial") // 多分类

val model = lr.fit(trainDF)
println(s"Coefficients: ${model.coefficientMatrix}")
println(s"Intercept: ${model.interceptVector}")
```

### 随机森林

```scala
val rf = new RandomForestClassifier()
  .setNumTrees(100)      // 树的数量
  .setMaxDepth(10)       // 最大深度
  .setMaxBins(32)        // 连续特征离散化桶数
  .setFeatureSubsetStrategy("sqrt")  // 每棵树使用的特征数比例
  .setImpurity("gini")   // 分裂标准

val model = rf.fit(trainDF)
println(s"Feature Importances: ${model.featureImportances}")
```

### GBDT (Gradient Boosted Trees)

```scala
val gbt = new GBTClassifier()
  .setMaxIter(50)        // 树的数量
  .setMaxDepth(5)
  .setStepSize(0.1)      // 学习率
```

## 聚类

### K-Means

```scala
val kmeans = new KMeans()
  .setK(5)              // 聚类数
  .setSeed(42)          // 随机种子
  .setMaxIter(50)
  .setInitMode("k-means||")  // 初始化方式

val model = kmeans.fit(featuresDF)
println(s"Cluster Centers: ${model.clusterCenters.mkString(", ")}")
// 预测
val predictions = model.transform(featuresDF)

// 评估 — 肘部法则选 K
(2 to 10).map { k =>
  val km = new KMeans().setK(k).setSeed(42)
  val m = km.fit(featuresDF)
  (k, m.summary.trainingCost)
}.foreach(println)
```

## 推荐算法 (ALS)

```scala
val als = new ALS()
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")
  .setRank(10)          // 隐因子维度
  .setMaxIter(20)
  .setRegParam(0.05)    // 正则化系数
  .setAlpha(1.0)        // 隐式反馈置信度
  .setImplicitPrefs(false)  // 是否隐式反馈

val model = als.fit(trainDF)
// 为用户推荐
val recs = model.recommendForUserSubset(usersDF, 10)
// 为物品推荐用户
val userRecs = model.recommendForItemSubset(itemsDF, 10)
```

## 模型评估

```scala
// 分类评估
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("accuracy")  // f1, weightedPrecision, weightedRecall

val accuracy = evaluator.evaluate(predictions)

// 回归评估
val regEval = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse")  // mse, r2, mae

// 交叉验证
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
```

## 面试高频考点

### Q: MLlib 为什么要从 RDD API 迁移到 DataFrame API？

1. DataFrame 有 Schema，类型信息更丰富
2. Catalyst 优化器可做全局优化
3. Pipeline API 统一了特征工程和模型训练
4. Tungsten 堆外内存减少 GC 开销
5. 和其他 Spark 组件（SQL、Streaming）更容易混合使用

### Q: 随机森林中树的深度和数量如何影响模型？

- **树太深**：容易过拟合（每棵树记住了训练数据）
- **树太浅**：欠拟合（学习不够）
- **树越多**：集成效果越好，但计算量线性增加，边际收益递减
- 常用调参策略：先调整 maxDepth 找到合适的深度，再增加 numTrees

### Q: K-Means 的 k 值如何选？

常用肘部法则 + 业务解释性：
1. 从 k=2 开始递增
2. 记录每个 k 的 SSE（误差平方和）
3. 找到 SSE 下降变缓的拐点

### Q: Spark ALS 和单机 ALS 的区别？

Spark ALS 的核心挑战在于**分布式矩阵分解**。spark.ml 的 ALS 使用块迭代优化（Block Partition），按用户/物品分块后分发到不同 Executor 上并行更新因子矩阵。

## 小结

| 组件 | 用途 |
|------|------|
| Pipeline | 统一工作流，包含 Transformer + Estimator |
| 特征工程 | StringIndexer → OneHotEncoder → VectorAssembler → Scaler |
| 分类 | LogisticRegression, RandomForest, GBT, NaiveBayes |
| 聚类 | KMeans, BisectingKMeans, LDA, GaussianMixture |
| 推荐 | ALS |
| 评估 | CrossValidator, TrainValidationSplit |
