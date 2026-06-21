# Spark MLlib — 高级算法

## 高级算法的应用场景

Spark MLlib 除了基础的分类回归和聚类之外，还提供了一些"杀手级"的高级算法，特别适合特定的业务场景。这些算法虽然不像 LR、RF 那样天天用，但在购物篮分析、序列挖掘、多分类等场景中是无可替代的：

| 算法 | 一句话描述 | 典型应用场景 |
|------|-----------|-------------|
| FP-Growth | 发现商品之间的关联关系 | "买了啤酒的人也买了尿布" |
| PrefixSpan | 发现用户行为的序列模式 | "登录→搜索→购买"转化路径 |
| PCA | 高维数据降维 | 可视化、减少计算量 |
| OneVsRest | 将多分类转为多个二分类 | 手写数字识别 |
| Isolation Forest | 异常检测（需手动实现） | 欺诈检测、故障预警 |

## 频繁模式挖掘（Frequent Pattern Mining）

### FP-Growth

#### 为什么要用 FP-Growth？

传统的关联规则挖掘算法 Apriori 有一个致命的问题：它需要反复扫描数据集来生成候选集，并且每次扫描都需要判断候选集是否为频繁项集。如果数据量很大（比如千万级交易记录），或者最小支持度设置得较低，候选集的数量会**指数级爆炸**——这就是 Apriori 的瓶颈。

**FP-Growth 解决了这个问题**，它只需要**两次扫描数据集**：
1. 第一次扫描：统计每个物品的频率，过滤掉不满足最小支持度的物品
2. 第二次扫描：构建 FP-Tree（频繁模式树），直接在树上挖掘频繁项集

```scala
import org.apache.spark.ml.fpm.FPGrowth

// FP-Growth 发现频繁项集（购物篮分析经典算法）
// 输入：每个 transaction 的物品列表

val transactions = spark.createDataFrame(Seq(
  (1, Array("牛奶", "面包", "鸡蛋")),
  (2, Array("面包", "黄油")),
  (3, Array("牛奶", "鸡蛋")),
  (4, Array("牛奶", "面包", "黄油", "鸡蛋")),
  (5, Array("面包", "鸡蛋"))
)).toDF("id", "items")

val fpGrowth = new FPGrowth()
  .setItemsCol("items")
  .setMinSupport(0.4)      // 最小支持度（出现频率 ≥ 40%）
  .setMinConfidence(0.6)   // 最小置信度（关联规则可靠性 ≥ 60%）
  .setNumPartitions(2)     // 分区数

val model = fpGrowth.fit(transactions)

// 频繁项集
model.freqItemsets.show()
// +---------------+----+
// |          items| freq|
// +---------------+----+
// |         [面包]|   4|
// |      [面包, 鸡蛋]|   3|
// |         [鸡蛋]|   4|
// |         [牛奶]|   3|
// |      [牛奶, 面包]|   2|
// |      [牛奶, 鸡蛋]|   3|
// |[牛奶, 面包, 鸡蛋]|   2|
// +---------------+----+

// 关联规则
model.associationRules.show()
// +----------+----------+------------------+
// |antecedent|consequent|        confidence|
// +----------+----------+------------------+
// |   [牛奶] |   [鸡蛋] |             1.0  |
// |   [牛奶] |   [面包] |0.6666666666666666|
// |   [面包] |   [鸡蛋] |            0.75  |
// +----------+----------+------------------+

// 根据关联规则预测（为已有物品推荐关联购买）
model.transform(transactions).show()
// +---+------------------------+-------------------------------+
// | id|     items              |         prediction            |
// +---+------------------------+-------------------------------+
// | 1 |[牛奶, 面包, 鸡蛋]      |[]                             |
// | 2 |[面包, 黄油]            |[]                             |
// | 3 |[牛奶, 鸡蛋]            |[]                             |
// | 4 |[牛奶, 面包, 黄油, 鸡蛋]|[]                             |
// | 5 |[面包, 鸡蛋]            |[牛奶]                         |
// +---+------------------------+-------------------------------+
// 注意：id=5 的用户买了面包和鸡蛋，预测推荐牛奶！
```

### 频繁项集的三个核心指标

| 指标 | 公式 | 含义 | 例子 |
|------|------|------|------|
| 支持度 (Support) | P(A∩B) | 所有交易中 A 和 B 同时出现的比例 | 牛奶和面包同时出现在 2/5=40% 的交易中 |
| 置信度 (Confidence) | P(B\|A) | 买了 A 的用户中同时买 B 的比例 | 买了牛奶的用户中 66.7% 也买了面包 |
| 提升度 (Lift) | P(B\|A)/P(B) | A 对 B 的购买概率的提升倍数 | Lift>1 表示正相关，如买牛奶使买面包的概率提升 1.67 倍 |

> **面试点**：FP-Growth 比 Apriori 快在哪里？FP-Growth 只需要扫描两遍数据集，第一遍统计频繁项，第二遍构建 FP-Tree，直接在树上递归挖掘频繁项集。Apriori 需要反复扫描数据集生成候选集并剪枝。在大型数据集上，FP-Growth 通常比 Apriori 快 1~2 个数量级。

### FP-Growth 的参数调优

| 参数 | 含义 | 调优建议 |
|------|------|---------|
| `minSupport` | 最小支持度 | 0.01~0.1，数据量越大设得越低 |
| `minConfidence` | 最小置信度 | 0.5~0.8，越高规则越可靠但数量越少 |
| `numPartitions` | 分区数 | 根据数据量和集群资源调整 |

> **实际经验**：设置 `minSupport` 时要注意——如果淘宝全量交易数据（亿级）跑 FP-Growth，minSupport=0.01 可能产生上万个频繁项集。小数据集（几百条）则 minSupport=0.1 都很正常。建议先设一个较高的值看效果，逐步降低。

### PrefixSpan（序列模式挖掘）

FP-Growth 处理的是"购物篮"（无顺序的物品集合），PrefixSpan 处理的是**有顺序的行为序列**。比如用户的登录→搜索→购买路径分析：

```scala
import org.apache.spark.ml.fpm.PrefixSpan

// PrefixSpan 挖掘序列模式（用户行为序列分析）
// 输入：每个用户的时序行为序列

val sequences = spark.createDataFrame(Seq(
  (1, Array(Array("登录"), Array("浏览"), Array("购买"))),
  (2, Array(Array("登录"), Array("搜索"), Array("浏览"), Array("购买"))),
  (3, Array(Array("搜索"), Array("浏览"))),
  (4, Array(Array("登录"), Array("购买")))
)).toDF("id", "sequences")

val prefixSpan = new PrefixSpan()
  .setSequenceCol("sequences")
  .setMinSupport(0.5)         // 最小支持度
  .setMaxPatternLength(5)     // 最大模式长度
  .setMaxLocalProjDBSize(32000000L)

val model = prefixSpan.findFrequentSequentialPatterns(sequences)
model.show()
// +--------------------+----+
// |         sequence   | freq|
// +--------------------+----+
// |         [[登录]]    |   3|
// |         [[浏览]]    |   3|
// |         [[购买]]    |   3|
// |    [[登录], [购买]] |   2|
// | [[登录], [浏览]]    |   2|
// +--------------------+----+
// 发现："登录 → 购买" 序列出现 2 次，是常见的用户转化路径
```

FP-Growth vs PrefixSpan 的应用场景对比：

| 对比维度 | FP-Growth | PrefixSpan |
|---------|-----------|-----------|
| 数据结构 | 无序集合（购物篮） | 有序序列（行为路径） |
| 典型输出 | {牛奶, 面包} | [登录] → [购买] |
| 业务场景 | 商品关联推荐、搭配推荐 | 用户行为路径分析、漏斗分析 |
| 算法复杂度 | O(items × transactions) | O(patterns × sequences) |

## 特征降维

### PCA（主成分分析）

PCA 是降维领域最经典的无监督方法。它通过线性变换，把高维数据投影到方差最大的几个方向上，达到降维目的：

```scala
import org.apache.spark.ml.feature.PCA

val pca = new PCA()
  .setInputCol("features")
  .setOutputCol("pcaFeatures")
  .setK(3)  // 降到 3 维

val pcaModel = pca.fit(df)
println(s"Explained variance ratio: ${pcaModel.explainedVariance}")
// 输出每个主成分解释的方差比例
// 比如 [0.7, 0.2, 0.05] 表示前 3 个主成分解释了 95% 的方差

val pcaDF = pcaModel.transform(df)
```

PCA 的典型使用场景：

| 场景 | 说明 |
|------|------|
| 可视化 | 降到 2/3 维，在坐标图中观察数据分布 |
| 去噪 | 丢弃方差小的主成分（那些往往是噪声） |
| 加速 | 降低特征维度后，后续模型训练更快 |
| 缓解过拟合 | 减少特征数量，降低模型复杂度 |

> **面试点**：PCA 的输出怎么解释？答案是：**通常不去解释**。每个主成分是原始特征的线性组合，很难赋予业务含义。在实际中，PCA 更常被当作一个"黑盒"降维工具使用。只有需要可解释性的场景（如金融风控），才会用其他方法替代 PCA。

## 异常检测

### Isolation Forest（Spark 3.x 无原生支持）

Spark MLlib 没有内置的 Isolation Forest，但可以通过随机森林的预测残差来实现一种简单的异常检测：

```scala
// 基于随机森林的异常检测思路
// 1. 用全部数据训练随机森林
// 2. 计算每个样本在所有树中的平均深度
// 3. 深度异常小的样本 → 异常点

import org.apache.spark.ml.regression.{RandomForestRegressor, RandomForestRegressionModel}

// 用随机森林的异常检测辅助
val rf = new RandomForestRegressor()
  .setFeaturesCol("features")
  .setNumTrees(100)
  .setMaxDepth(10)

// 通过模型预测残差大小判断异常
val model = rf.fit(trainDF)
val predictions = model.transform(testDF)
val anomalies = predictions
  .withColumn("residual", abs($"label" - $"prediction"))
  .filter($"residual" > threshold)
```

> 生产环境中异常检测更常用专门的库（如 PyOD、scikit-learn IsolationForest），数据量大时结合 Spark 做特征工程和预处理。如果你一定要在 Spark 中做异常检测，可以考虑使用 `sparklyr` 或自行实现 Isolation Forest。

## 模型集成与 Pipelines 进阶

### OneVsRest（一对多分类）

当你的多分类问题中类别数很多（比如手写数字 0~9），而基础分类器是二分类器（如 SVM），OneVsRest 可以把多分类拆成多个二分类：

```scala
import org.apache.spark.ml.classification.{OneVsRest, LogisticRegression}

// OneVsRest 将多分类问题拆为多个二分类
// K 个类别 → K 个二分类器（当前类 vs 其余类）

val lr = new LogisticRegression()
  .setMaxIter(100)
  .setRegParam(0.01)

val ovr = new OneVsRest()
  .setClassifier(lr)
  .setLabelCol("label")
  .setFeaturesCol("features")
  .setParallelism(4)  // 并行训练 K 个二分类器

val ovrModel = ovr.fit(trainDF)
val predictions = ovrModel.transform(testDF)

// 预测结果中，probability 列改为每个类别的概率
predictions.select("prediction", "probability").show(5)
```

| 多分类策略 | 原理 | 训练分类器数 | 适用场景 |
|-----------|------|-------------|---------|
| OneVsRest | 每个类 vs 其余所有类 | K 个 | 类别不太多（<100） |
| OneVsOne | 每两个类之间训练一个 | K×(K-1)/2 个 | 类别不多且分类器训练快 |

### 模型融合（Ensemble）

Spark MLlib 没有内置的 Voting/Averaging 集成器，但可以通过 DataFrame 的 Join 操作手动实现：

```scala
// 方案：训练多个模型，对预测结果做平均/投票
import org.apache.spark.sql.functions._

// 分别训练
val rfModel = rf.fit(trainDF)
val lrModel = lr.fit(trainDF)
val gbtModel = gbt.fit(trainDF)

// 分别预测，保留概率列
val rfPred = rfModel.transform(testDF)
  .select("id", "probability")
  .withColumnRenamed("probability", "rf_prob")

val lrPred = lrModel.transform(testDF)
  .select("id", "probability")
  .withColumnRenamed("probability", "lr_prob")

val gbtPred = gbtModel.transform(testDF)
  .select("id", "probability")
  .withColumnRenamed("probability", "gbt_prob")

// 平均概率——简单但有效
val ensemble = rfPred.join(lrPred, "id").join(gbtPred, "id")
  .withColumn("avg_prob", (col("rf_prob") + col("lr_prob") + col("gbt_prob")) / 3)
```

## MLlib 的局限性

虽然 MLlib 功能丰富，但以下场景它并不擅长，需要结合其他框架使用：

```scala
// 1. 深度学习不支持
// Spark MLlib 没有神经网络/深度学习的完整支持
// 推荐：Spark 做 ETL + 特征工程 → TensorFlow/PyTorch 做训练

// 2. 在线学习不支持
// MLlib 只支持批量训练，不支持增量/在线学习
// 需要实时更新模型？考虑使用 Streaming 定时重训

// 3. 小数据量效率低
// 几百 MB 的数据 → 单机的 scikit-learn 更快更方便
// Spark 的分布式调度开销在小数据量下反而成为负担

// 4. 特征工程能力有限
// 复杂特征变换（交叉特征、时序特征）需要自定义 Transformer
// 建议：结合 Spark SQL 的 UDF 做复杂特征处理
```

MLlib  vs 专业 ML 框架的选型建议：

| 场景 | 推荐方案 | 原因 |
|------|---------|------|
| 数据处理量大（>1TB） | Spark MLlib | 分布式处理是唯一选择 |
| 数据量一般（<100GB） | scikit-learn | 单机更方便，调试容易 |
| 需要深度学习 | TensorFlow/PyTorch | MLlib 不支持 |
| 需要在线学习 | 自研或使用 Flink ML | MLlib 不支持 |
| 需要可解释性 | MLlib (LR/决策树) | 或者使用 SHAP/LIME |
| 快速原型验证 | scikit-learn + Pandas | Python 生态更丰富 |

## 面试高频考点

### Q: FP-Growth 和 Apriori 的区别？

FP-Growth 只需要扫描两次数据集，第一次统计频繁项，第二次构建 FP-Tree。Apriori 需要反复扫描生成候选集并剪枝，每多一层就要多一次全表扫描。在大型数据集上，FP-Growth 通常比 Apriori 快 1~2 个数量级。面试中可以说：FP-Growth 通过压缩数据结构（FP-Tree）避免了 Apriori 的候选集爆炸问题。

### Q: OneVsRest 的原理？

对于 K 分类问题，训练 K 个二分类器，每个分类器负责区分"当前类 vs 其他所有类"。预测时选择置信度最高的分类器的类别作为最终预测结果。优点是实现简单，缺点是和标签类别数量线性增长。

### Q: Spark MLlib 为什么没有深度神经网络？

Spark 的核心设计是分布式数据并行（Data Parallelism），而深度学习需要模型并行（Model Parallelism）和 GPU 加速。Spark 的架构假设数据分布在多台机器上、模型加载到内存中——这对深度学习来说效率太低（深度学习的核心瓶颈在计算，不在数据 I/O）。生产中通常用 Spark 做特征工程 + TensorFlow/PyTorch 做模型训练。

### Q: 什么是关联规则中的"啤酒与尿布"？

这是数据挖掘最经典的案例：超市通过分析购物篮数据发现，购买尿布的顾客经常也购买啤酒。原因分析是年轻父亲在给孩子买尿布时会顺带买啤酒犒劳自己。这个案例说明了关联规则挖掘的商业价值——通过发现商品之间的关联关系来优化商品摆放和交叉销售。

## 小结

| 算法 | 场景 | 输入 | 输出 |
|------|------|------|------|
| FP-Growth | 购物篮分析、物品关联推荐 | Transaction 列表 | 频繁项集 + 关联规则 |
| PrefixSpan | 用户行为序列分析、路径挖掘 | 时序行为序列 | 频繁序列模式 |
| PCA | 高维数据降维、可视化 | 稠密特征向量 | 降维后的特征向量 |
| OneVsRest | 多分类问题 | 标签 + 特征 | 多个二分类器 |
| 模型集成 | 提升预测稳定性 | 多个模型预测结果 | 平均/投票后的结果 |
