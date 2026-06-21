# Spark MLlib — 分类与回归

## 分类与回归概述

### 什么时候用分类？什么时候用回归？

这个问题看似简单，但确实很多初学者容易混淆。判断标准只有一个：**看你的目标变量（label）是什么类型**。

| 任务类型 | 目标变量 | 例子 | 评估指标 |
|---------|---------|------|---------|
| 二分类 | 两个离散值（0/1, 是/否） | 是否逾期、是否点击 | Accuracy, AUC, F1 |
| 多分类 | 多个离散值（A/B/C） | 手写数字识别 0~9 | Accuracy, F1 |
| 回归 | 连续值 | 房价预测、温度预测 | RMSE, MAE, R² |

> **面试点**：面试中经常会被问到"逻辑回归是回归还是分类？"答案是：名字里有回归，但实际是用来做分类的。它输出的是样本属于各个类别的概率，然后选择概率最大的类别作为预测结果。

## 分类模型

### 逻辑回归（Logistic Regression）

逻辑回归可以说是**工业界最常用的分类算法**。它的核心在于：
1. 使用 Sigmoid 函数将线性回归的输出映射到 [0, 1] 的概率区间
2. 通过最大似然估计（MLE）来优化参数
3. 可解释性极强——可以明确说出"收入每增加 1 万，违约概率增加 X%"

```scala
import org.apache.spark.ml.classification.LogisticRegression

val lr = new LogisticRegression()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setMaxIter(100)
  .setRegParam(0.01)        // L2 正则化系数（防止过拟合）
  .setElasticNetParam(0.0)  // 0=L2, 1=L1
  .setFamily("auto")        // auto / binomial(二分类) / multinomial(多分类)
  .setTol(1e-6)             // 收敛阈值
  .setStandardization(true) // 是否自动标准化特征（默认 true）

val lrModel = lr.fit(trainDF)

// 查看模型参数
println(s"Coefficients: ${lrModel.coefficientMatrix}")
println(s"Intercept: ${lrModel.interceptVector}")

// 训练集上的评估指标
println(s"Training accuracy: ${lrModel.summary.accuracy}")
```

逻辑回归的关键参数调优：

| 参数 | 说明 | 调优建议 |
|------|------|---------|
| `regParam` | 正则化强度 | 从 0.001 开始试，过拟合就增大 |
| `elasticNetParam` | L1/L2 混合比例 | 0 = 纯 L2, 1 = 纯 L1, 0.5 = 各一半 |
| `maxIter` | 最大迭代次数 | 100 ~ 500，数据量大时需要更多 |
| `tol` | 收敛阈值 | 默认 1e-6，精度要求不高可以调大到 1e-4 加速 |
| `family` | 分类类型 | binominal（二分类） / multinomial（多分类） |

### 逻辑回归的正则化

正则化是防止过拟合的关键手段，MLlib 通过 `elasticNetParam` 支持 L1 和 L2 的混合：

```scala
// L2 正则化（Ridge）—— 权重均匀收缩
val l2_lr = new LogisticRegression()
  .setElasticNetParam(0.0)  // 纯 L2
  .setRegParam(0.01)

// L1 正则化（Lasso）—— 产生稀疏权重（部分权重为 0，相当于特征选择）
val l1_lr = new LogisticRegression()
  .setElasticNetParam(1.0)  // 纯 L1

// ElasticNet —— L1 + L2 混合
val en_lr = new LogisticRegression()
  .setElasticNetParam(0.5)  // L1 和 L2 各贡献一半
```

> **面试点**：L1 和 L2 正则化的区别是非常高频的面试题。L1 会产生稀疏解（很多权重为 0），相当于在做特征选择。L2 会让权重均匀地趋近于 0 但不等于 0。当特征数量远大于样本量时，推荐用 L1。当大部分特征都有用时，用 L2。

### 随机森林（Random Forest）

随机森林是 Bagging 集成方法的代表，它的核心思想是：**训练多棵决策树，每棵树在数据和特征上都加入随机性，最后投票决定结果**。这种"众人拾柴火焰高"的思路让随机森林成为最"省心"的算法——不需要太多调参，效果常常就不错。

```scala
import org.apache.spark.ml.classification.RandomForestClassifier

val rf = new RandomForestClassifier()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setNumTrees(100)          // 树的数量（越多越好，但计算量线性增长）
  .setMaxDepth(10)           // 最大深度（太深容易过拟合）
  .setMinInstancesPerNode(1) // 叶子节点最少样本数
  .setMinInfoGain(0.0)       // 最小信息增益
  .setMaxBins(32)            // 连续特征离散化桶数
  .setSubsamplingRate(1.0)   // 每棵树使用的样本比例
  .setFeatureSubsetStrategy("sqrt")  // 每棵树特征采样策略
  .setImpurity("gini")       // 分裂标准：gini / entropy
  .setSeed(42)

val rfModel = rf.fit(trainDF)

// 特征重要性（这个信息非常有用！）
println(s"Feature importances: ${rfModel.featureImportances}")
// 值越大 → 该特征对分类越重要
// 可用于指导特征筛选——重要性接近 0 的特征可以考虑删除
```

随机森林的调参指南：

| 参数 | 作用 | 调优经验 |
|------|------|---------|
| `numTrees` | 树的棵树 | 100 ~ 500，500 棵以后收益递减明显 |
| `maxDepth` | 每棵树的最大深度 | 10 ~ 20，谨慎增大，太深容易过拟合 |
| `minInstancesPerNode` | 叶子节点最小样本数 | 数据量大时设为 1%~5% 的样本量 |
| `featureSubsetStrategy` | 特征采样 | 分类默认 sqrt，回归默认 n/3 |
| `impurity` | 分裂标准 | gini 和 entropy 效果接近 |

### 随机森林的 Bagging 原理

随机森林的随机性体现在两个层面：

1. **数据随机性（Bootstrap）**：从 N 个训练样本中有放回地抽取 N 个作为每棵树的训练集，平均每个样本有 63.2% 的概率被抽到
2. **特征随机性**：每棵树分裂时，只从全部 M 个特征中随机选择 m 个来评估（分类 m = √M，回归 m = M/3）

这两层随机性保证了树之间的多样性——不相关的树组成的集成，比相关的树效果更好。

### GBDT（梯度提升树）

GBDT 采用和随机森林完全不同的集成思路——**Boosting**。它的核心思想是：每棵新树去拟合前一棵树的残差（或梯度方向）。可以理解为：一个人做不好的事，让第二个人去纠正第一个人的错误，第三个人去纠正前两个人的错误，以此类推。

```scala
import org.apache.spark.ml.classification.GBTClassifier

val gbt = new GBTClassifier()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setMaxIter(50)       // 提升迭代次数（树的数量）
  .setMaxDepth(5)       // 每棵树的深度（GBDT 的树通常较浅）
  .setStepSize(0.1)     // 学习率（小步长 + 多迭代 = 更好效果）
  .setSubsamplingRate(0.8)  // 每次迭代的样本采样率
  .setLossType("logistic")  // logistic / exponential

val gbtModel = gbt.fit(trainDF)
```

随机森林 vs GBDT 的对比：

| 对比维度 | 随机森林 | GBDT |
|---------|---------|------|
| 集成方式 | Bagging（并行训练） | Boosting（串行训练） |
| 训练速度 | 快（可以并行） | 慢（必须串行） |
| 抗过拟合 | 强 | 弱（容易过拟合） |
| 对异常值 | 鲁棒 | 敏感 |
| 参数敏感度 | 低（默认参数效果还行） | 高（需要仔细调参） |
| 精度上限 | 中等 | 高 |
| 特征缩放 | 不需要 | 不需要 |
| 适合数据类型 | 高维稀疏数据 | 低维稠密数据 |

> **面试点**："随机森林 vs GBDT 的区别"是面试中的经典问题。回答时抓住核心区别：一个用 Bagging 并行降低方差，一个用 Boosting 串行降低偏差。随机森林的每棵树独立训练，它们之间没有依赖关系。GBDT 的每棵树依赖于前一棵树的结果，所以不能并行。

### 朴素贝叶斯（Naive Bayes）

朴素贝叶斯的核心假设：**特征之间相互独立**。虽然这个假设在现实中基本不成立，但在文本分类等场景下效果出奇得好。

```scala
import org.apache.spark.ml.classification.NaiveBayes

val nb = new NaiveBayes()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setModelType("multinomial")  // multinomial / bernoulli / gaussian
  .setSmoothing(1.0)            // 拉普拉斯平滑（防止概率为 0）

val nbModel = nb.fit(trainDF)
```

朴素贝叶斯的三种模型变体：

| 模型类型 | 适用场景 | 说明 |
|---------|---------|------|
| multinomial | 文本分类（词频特征） | 适用于离散计数特征 |
| bernoulli | 文本分类（词是否出现） | 适用于二值特征（0/1） |
| gaussian | 连续特征 | 假设特征服从高斯分布 |

## 回归模型

### 线性回归（Linear Regression）

线性回归是最简单的回归模型，它假设目标变量和特征之间存在线性关系。虽然简单，但在很多场景下效果够用——而且可解释性极强。

```scala
import org.apache.spark.ml.regression.LinearRegression

val lr = new LinearRegression()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setMaxIter(100)
  .setRegParam(0.3)        // 正则化
  .setElasticNetParam(0.8) // 0=L2(Ridge), 1=L1(Lasso)
  .setLoss("squaredError") // 损失函数：squaredError / huber
  .setStandardization(true) // 自动标准化

val lrModel = lr.fit(trainDF)

// 模型参数
println(s"Coefficients: ${lrModel.coefficients}")
println(s"Intercept: ${lrModel.intercept}")

// 模型评估
println(s"RMSE: ${lrModel.summary.rootMeanSquaredError}")
println(s"R²: ${lrModel.summary.r2}")
```

线性回归的损失函数：

| 损失函数 | 说明 | 适用场景 |
|---------|------|---------|
| `squaredError` | 均方误差（MAE 的平方） | 数据干净，无异常值 |
| `huber` | Huber 损失 | 数据存在异常值，比 squaredError 鲁棒 |

### 决策树回归（Decision Tree Regression）

```scala
import org.apache.spark.ml.regression.DecisionTreeRegressor

val dt = new DecisionTreeRegressor()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setMaxDepth(10)
  .setMinInstancesPerNode(5)
  .setImpurity("variance")
```

### 随机森林回归

```scala
import org.apache.spark.ml.regression.RandomForestRegressor

val rf = new RandomForestRegressor()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setNumTrees(100)
  .setMaxDepth(10)
  .setMinInstancesPerNode(5)

val rfModel = rf.fit(trainDF)
println(s"Feature importances: ${rfModel.featureImportances}")
println(s"RMSE: ${rfModel.summary.rootMeanSquaredError}")
```

## 分类 vs 回归

| 维度 | 分类 | 回归 |
|------|------|------|
| 目标变量 | 离散（0/1, A/B/C） | 连续（价格、温度） |
| 评估指标 | Accuracy, F1, AUC, Precision, Recall | RMSE, MAE, R², MSE |
| 常用算法 | LR, RF, GBDT, NB | LinearReg, RF Regressor, GBT Regressor |
| 模型输出 | 类别 + 概率 | 实数值 |
| 损失函数 | LogLoss, CrossEntropy | MSE, MAE, Huber |
| 典型应用 | 信用评分、垃圾邮件检测、图像分类 | 房价预测、销量预测、温度预测 |

## 示例：完整的分类 Pipeline

下面是一个从数据加载到模型评估的完整示例，涵盖了特征工程、模型训练、超参数调优的全流程：

```scala
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, StandardScaler}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

// 1. 准备数据
val data = spark.read.parquet("hdfs://data/ml_data")

// 划分训练集和测试集
val Array(trainDF, testDF) = data.randomSplit(Array(0.8, 0.2), seed = 42)

// 2. 定义 Pipeline
val labelIndexer = new StringIndexer()
  .setInputCol("label_str").setOutputCol("label")

val assembler = new VectorAssembler()
  .setInputCols(Array("age", "income", "education", "city_vec"))
  .setOutputCol("rawFeatures")

val scaler = new StandardScaler()
  .setInputCol("rawFeatures").setOutputCol("features")
  .setWithStd(true).setWithMean(false)  // 稀疏数据不需要减均值

val rf = new RandomForestClassifier()
  .setFeaturesCol("features").setLabelCol("label")

val pipeline = new Pipeline()
  .setStages(Array(labelIndexer, assembler, scaler, rf))

// 3. 网格搜索
val paramGrid = new ParamGridBuilder()
  .addGrid(rf.numTrees, Array(50, 100, 200))
  .addGrid(rf.maxDepth, Array(5, 10, 15))
  .build()

// 4. 交叉验证
val evaluator = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setMetricName("f1")

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)
  .setParallelism(3)  // 并行训练 3 个模型

// 5. 训练（自动进行网格搜索 + 交叉验证）
println("Training with CrossValidator...")
val cvModel = cv.fit(trainDF)

// 6. 评估
val predictions = cvModel.transform(testDF)
val f1 = evaluator.evaluate(predictions)
println(s"Test F1: $f1")

// 查看最佳参数
val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
val bestRF = bestModel.stages.last.asInstanceOf[RandomForestClassificationModel]
println(s"Best numTrees: ${bestRF.getNumTrees}")
println(s"Best maxDepth: ${bestRF.getMaxDepth}")
```

## 面试高频考点

### Q: 随机森林和 GBDT 的区别？

- **随机森林**：Bagging 集成，每棵树独立并行训练，对异常值鲁棒，不容易过拟合。通过降低方差提升效果。
- **GBDT**：Boosting 集成，每棵树拟合前一棵的残差，串行训练，容易过拟合但精度可能更高。通过降低偏差提升效果。
- 随机森林适合高维稀疏数据，GBDT 适合低维稠密数据。
- 随机森林调参相对简单，GBDT 需要仔细调整学习率和迭代次数的平衡。

### Q: 逻辑回归为什么要做特征标准化？

逻辑回归使用梯度下降优化，特征标准化后各维度的更新步长一致，收敛更快。具体来说，如果收入范围 0~100 万，年龄范围 0~100，那么收入对应的梯度会远大于年龄对应的梯度——导致优化器在收入维度上大步前进，在年龄维度上小步挪动，收敛路径曲折。标准化后，所有特征在同一尺度上，梯度下降的效率会大幅提升。

### Q: L1 和 L2 正则化的区别？

- **L1（Lasso）**：产生稀疏权重（部分特征权重为 0），可以做特征选择。在特征数量很大但大部分和标签无关时效果好。
- **L2（Ridge）**：权重均匀收缩（接近 0 但不为 0），防止过拟合。在大部分特征都有用时效果好。
- ElasticNet（`elasticNetParam` ∈ [0,1]）是 L1 + L2 的组合，结合了两者的优点。

### Q: 数据不平衡怎么处理？

数据不平衡是工业界的常见问题（如欺诈检测中正样本只有 1%）。常用解决方案：
1. **欠采样**：减少多数类的样本量
2. **过采样**：增加少数类的样本（如 SMOTE）
3. **调整 class weight**：给少数类更高的权重
4. **使用 AUC 评估**：Accuracy 会被多数类主导
5. **集成方法**：训练多个模型，每个模型使用不同的采样数据

## 小结

| 算法 | 类型 | 特点 | 适用场景 |
|------|------|------|---------|
| LogisticRegression | 分类 | 可解释性强，输出概率 | 风控、医疗诊断 |
| RandomForest | 分类/回归 | 鲁棒，不需要太多调参 | 通用场景 |
| GBDT | 分类/回归 | 精度高，容易过拟合 | 竞赛、CTR 预估 |
| NaiveBayes | 分类 | 快，适合高维文本 | 文本分类、垃圾邮件检测 |
| LinearRegression | 回归 | 可解释性强 | 价格预测、趋势预测 |
| DecisionTree | 分类/回归 | 可视化好，容易过拟合 | 快速基线模型 |
