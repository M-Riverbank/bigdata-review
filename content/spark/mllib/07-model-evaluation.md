# Spark MLlib — 模型评估与调参

## 模型评估总览

### 为什么要重视模型评估？

很多人容易陷进一个误区：模型训练完了就完事了，随便看个 Accuracy 就觉得项目完成了。但实际上，**模型评估比模型训练更需要经验**——不同的业务场景需要不同的评估指标，错误地选择评估指标可能导致同样一个模型被评估为"优秀"或"糟糕"。

比如在信用卡欺诈检测中，如果正样本（欺诈）只占 0.1%，你把所有数据都判为正常——Accuracy 高达 99.9%，看起来完美！但这个模型完全没有发现任何欺诈交易，毫无商业价值。这个例子说明了：**在类别不平衡时，Accuracy 是一个误导性极强的指标**。

不同类型的模型使用不同的评估指标：

```
分类问题
├── Accuracy（准确率）     = (TP+TN) / (TP+TN+FP+FN)
├── Precision（精确率）    = TP / (TP+FP)
├── Recall（召回率）       = TP / (TP+FN)
├── F1（调和平均）         = 2 × P × R / (P + R)
├── AUC（ROC 曲线下面积）
└── LogLoss（对数损失）

回归问题
├── RMSE（均方根误差）
├── MAE（平均绝对误差）
├── R²（决定系数）
└── MSE（均方误差）

聚类问题
├── 轮廓系数（Silhouette）
└── SSE（误差平方和）
```

### 混淆矩阵的概念

理解分类评估指标，首先要理解混淆矩阵：

|  | 预测为正类 | 预测为负类 |
|--|-----------|-----------|
| **实际为正类** | TP（真正例） | FN（假负例） |
| **实际为负类** | FP（假正例） | TN（真负例） |

根据这个矩阵，可以推导出所有分类指标：

| 指标 | 计算公式 | 关注点 |
|------|---------|--------|
| Accuracy | (TP+TN)/(TP+TN+FP+FN) | 整体正确率 |
| Precision | TP/(TP+FP) | 预测为正类的样本中有多少是真的正类 |
| Recall | TP/(TP+FN) | 真正的正类中有多少被找出来了 |
| F1 | 2×P×R/(P+R) | Precision 和 Recall 的调和平均 |
| Specificity | TN/(TN+FP) | 负类的识别率 |

## 分类评估

### 代码实现

```scala
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

// 二分类评估
val binaryEval = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("rawPrediction")  // 或 probability
  .setMetricName("areaUnderROC")         // AUC

val auc = binaryEval.evaluate(predictions)
println(s"AUC: $auc")

// 多分类评估
val multiEval = new MulticlassClassificationEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("f1")  // accuracy, f1, weightedPrecision, weightedRecall

val f1 = multiEval.evaluate(predictions)
println(s"F1 Score: $f1")
```

### 混淆矩阵

Spark 没有直接提供混淆矩阵的 API，但可以通过 DataFrame 的聚合操作来计算：

```scala
// 方法 1：groupBy 统计
predictions.groupBy("label", "prediction")
  .count()
  .orderBy("label", "prediction")
  .show()

// 方法 2：交叉表（更直观）
predictions.crosstab("label", "prediction").show()
// +------------+---+---+---+
// |label_prediction|  0|  1|  2|
// +------------+---+---+---+
// |           0| 85|  5| 10|
// |           1|  8| 72| 20|
// |           2|  3| 12| 85|
// +------------+---+---+---+
// 对角线上的数字（85, 72, 85）是正确预测的样本数
// 非对角线的是错误预测的样本数
```

### 分类指标的选择指南

| 场景 | 关注指标 | 原因 |
|------|---------|------|
| 类别均衡 | Accuracy | 简单直观 |
| 类别严重不平衡 | AUC / F1 | Accuracy 会被多数类主导 |
| 欺诈检测 | Recall（召回率） | 宁可误报，不可漏报 |
| 搜索结果 | Precision（精确率） | 宁缺毋滥，不要给用户垃圾结果 |
| 需要平衡 | F1 | Precision 和 Recall 的折中 |

> **面试点**：Accuracy 和 F1 哪个好？Accuracy 在类别平衡时直观好理解，但类别不平衡时误导性强。F1 是 Precision 和 Recall 的调和平均，对不平衡类别更鲁棒。在工业界，**AUC 是最常用的二分类评估指标**，因为它不依赖阈值选择。

## 回归评估

```scala
import org.apache.spark.ml.evaluation.RegressionEvaluator

val eval = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse")  // rmse, mse, r2, mae, var

val rmse = eval.evaluate(predictions)
println(s"RMSE: $rmse")
```

### 不同指标的含义

| 指标 | 公式 | 说明 | 范围 | 特点 |
|------|------|------|------|------|
| MSE | mean((yi - ŷi)²) | 均方误差 | ≥ 0，越小越好 | 对大误差惩罚更大（因为平方） |
| RMSE | sqrt(MSE) | 均方根误差 | ≥ 0，越小越好 | 单位与原数据相同，更直观 |
| MAE | mean(|yi - ŷi|) | 平均绝对误差 | ≥ 0，越小越好 | 对异常值不敏感，更鲁棒 |
| R² | 1 - SSres/SStot | 决定系数 | [0,1]，越大越好 | 模型解释了多少方差 |

### 指标的选择建议

| 场景 | 推荐指标 | 原因 |
|------|---------|------|
| 数据干净无异常值 | RMSE | 大误差更敏感，全面评估 |
| 数据有异常值 | MAE | 不受少数极值影响 |
| 需要比较不同 scale 的数据 | R² | 无量纲，[0,1] 范围 |
| 最终报告 | R² + RMSE 一起 | R² 说明解释力，RMSE 说明实际误差大小 |

## 超参数调优

### 为什么需要超参数调优？

机器学习模型的参数分两种：一种是模型**在训练过程中学习到的**参数（如逻辑回归的系数），另一种是**训练前就要设定**的参数（如随机森林的树数量）。后者就是"超参数"。超参数的选择直接影响模型效果：

| 超参数设得太小 | 超参数设得太大 |
|--------------|--------------|
| maxDepth=2 → 欠拟合 | maxDepth=30 → 过拟合 |
| numTrees=10 → 不稳定 | numTrees=1000 → 训练慢 |
| regParam=0 → 过拟合 | regParam=1 → 欠拟合 |

Spark MLlib 提供三种调参策略：

### 1. CrossValidator（交叉验证）

CrossValidator 是**最稳定**的调参方式，它把数据分成 K 份，轮流用 K-1 份训练、1 份验证：

```scala
import org.apache.spark.ml.tuning.CrossValidator

// 将数据分为 K 份，K-1 份训练，1 份验证，轮转 K 次
// 结果取 K 次评估的平均值——这样评估结果更可靠

val cv = new CrossValidator()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(5)       // 5 折交叉验证
  .setParallelism(3)    // 并行训练的模型数
  .setSeed(42)

val cvModel = cv.fit(trainDF)

// 最佳模型的参数
val bestModel = cvModel.bestModel
// Pipeline 的最后 stage 是 RF 模型
bestModel.asInstanceOf[PipelineModel]
  .stages.last.explainParams()
```

### 2. TrainValidationSplit（训练验证划分）

当数据量很大时（几百 GB 以上），交叉验证的 K 倍计算量可能无法接受。这时候可以用 TrainValidationSplit：

```scala
import org.apache.spark.ml.tuning.TrainValidationSplit

// 一次划分：大部分训练，小部分验证
// 比 CrossValidator 快，但评估不稳定（结果可能因划分方式而异）

val tvs = new TrainValidationSplit()
  .setEstimator(pipeline)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setTrainRatio(0.8)    // 80% 训练，20% 验证
  .setParallelism(3)

val tvsModel = tvs.fit(trainDF)
```

### CrossValidator vs TrainValidationSplit

| 维度 | CrossValidator | TrainValidationSplit |
|------|---------------|---------------------|
| 数据利用 | 充分利用（所有数据既训练又验证） | 浪费 20% 的训练数据 |
| 评估稳定性 | 稳定（K 次评估取平均） | 不稳定（单次划分） |
| 训练时间 | K 倍训练时间 | 1 次训练时间 |
| 小数据集（<10GB） | 推荐 | 不推荐 |
| 中等数据（10~100GB） | 推荐（K=3） | 可选 |
| 大数据集（>100GB） | 不推荐 | 推荐 |

### CrossValidator 的计算量估算

假设你的 ParamGrid 有 18 种参数组合，5 折交叉验证：
- 需要训练的模型数：18 × 5 = 90 个模型
- 如果每个模型训练 3 分钟：90 × 3 = 270 分钟（4.5 小时）

> **实际经验**：网格搜索的计算量非常大。在实际项目中，可以先在大范围上粗搜（比如 maxDepth 试 [5, 10, 15, 20]），然后在小范围上精搜（比如找到最佳 maxDepth=10，再在 [8, 9, 10, 11, 12] 上搜一次）。

## 模型持久化

训练好的模型可以持久化保存，并在生产环境中加载使用：

```scala
// 保存训练好的模型
model.write.overwrite().save("hdfs://models/rf_model")

// 加载模型
import org.apache.spark.ml.classification.RandomForestClassificationModel
val loadedModel = RandomForestClassificationModel.load("hdfs://models/rf_model")

// 加载 PipelineModel
import org.apache.spark.ml.PipelineModel
val pipelineModel = PipelineModel.load("hdfs://models/rf_pipeline")
```

模型版本管理的最佳实践：

```scala
// 推荐目录结构：
// hdfs://models/
//   ├── v1_20240101_feature_v1/   # 初始版本
//   ├── v2_20240201_feature_v2/   # 特征优化后
//   └── prod_current/             # 符号链接或软路由指向当前版本

// 代码中引用固定路径
val prodModel = PipelineModel.load("hdfs://models/prod_current")
// 上线新版本时，只需要更新 prod_current 的指向
```

## 面试高频考点

### Q: Accuracy 和 F1 哪个好？

Accuracy 在类别平衡时直观好理解，但类别不平衡时误导性强（99% 负样本 → 全判负的 Accuracy=99%，但模型毫无价值）。F1 是 Precision 和 Recall 的调和平均，对不平衡类别更鲁棒。**在实际工业场景中**，大多数分类问题的数据都是不平衡的（正常交易 > 欺诈，正常用户 > 流失用户），所以 F1 和 AUC 比 Accuracy 更常用。

### Q: 交叉验证的 K 怎么选？

K 的选择是偏差和方差的权衡：
- **K=3**：计算量小但评估可能不准确（偏差大）
- **K=5**：常用的折中（推荐，偏差和方差平衡）
- **K=10**：评估稳定但计算量大（方差小）
- **留一法（K=N）**：最稳定但计算量巨大，只在数据量很小时使用

通用建议：数据量 < 10000 条选 K=10，10000~10 万条选 K=5，> 10 万条选 K=3 或改用 TrainValidationSplit。

### Q: 模型过拟合的表现和解决办法？

**表现**：训练集精度很高但测试集精度差；训练集误差持续下降但验证集误差停止下降甚至开始上升。

**解决办法**：
1. 增加正则化（增大 `regParam`）
2. 减少特征（降维/特征选择）
3. 增加训练数据
4. 降低模型复杂度（减小 `maxDepth`、增加 `minInstancesPerNode`）
5. 增加随机性（随机森林中增大 `subsamplingRate` < 1.0）
6. 早停法（Early Stopping）

### Q: 什么时候用 AUC 而不是 Accuracy？

数据不平衡时一定要用 AUC！AUC 不依赖分类阈值（不需要设 0.5 还是 0.3），它衡量的是模型整体的排序能力——"正样本排在负样本前面的概率"。此外，AUC 对类别比例不敏感，在不同比例的数据上具有可比性。

## 小结

| 问题类型 | 主要评估指标 | 调参方法 |
|---------|-------------|---------|
| 二分类 | AUC, F1, Accuracy | CrossValidator 或 TVS |
| 多分类 | F1, Accuracy | CrossValidator 或 TVS |
| 回归 | RMSE, R², MAE | CrossValidator 或 TVS |
| 聚类 | Silhouette, SSE | 肘部法、轮廓系数 |
| 推荐 | RMSE, MAP | CrossValidator（影响大） |
