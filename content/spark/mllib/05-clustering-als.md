# Spark MLlib — 聚类与推荐

## 聚类与推荐的业务价值

### 为什么把聚类和推荐放在一起讲？

聚类和推荐是 Spark MLlib 在工业界应用最广泛的场景之一。聚类用于"无中生有"地发现数据中的结构（用户分群、异常检测），推荐用于"投其所好"地预测用户偏好（商品推荐、内容推荐）。

两者的共同点：都是**无监督或半监督**的学习方法——不需要人工标注的标签，直接从数据本身发现模式。

| 应用领域 | 聚类场景 | 推荐场景 |
|---------|---------|---------|
| 电商 | 用户价值分群（RFM 模型） | "买了也买了"商品推荐 |
| 内容平台 | 文章主题聚类 | 个性化内容推荐 |
| 金融 | 异常交易检测 | 理财产品推荐 |
| 社交 | 社交圈层发现 | 好友推荐 |
| 广告 | 用户画像分群 | 广告定向投放 |

## K-Means 聚类

### 算法原理

K-Means 是最常用的聚类算法，它的目标是把数据分成 K 个簇，让每个点都属于离它最近的簇中心。算法的核心步骤只有 4 步：

1. 随机选择 K 个初始中心
2. 计算每个点到 K 个中心的距离，归属到最近的簇
3. 更新每个簇的中心（取簇内所有点的均值）
4. 重复 2-3 步直到收敛

Spark MLlib 的实现使用 **K-Means||**（K-Means Parallel）初始化策略，比传统的随机初始化更适合分布式环境——它通过多次采样更均匀地选择初始中心，减少迭代次数。

> **面试点**：K-Means|| 是 K-Means++ 的并行化版本。K-Means++ 需要串行选择初始中心点，在大数据量下效率低。K-Means|| 通过 Over-sampling 方式并行选择候选中心，再从中选出 K 个最优的，兼顾了初始质量和分布式效率。

### 代码实现

```scala
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator

val kmeans = new KMeans()
  .setFeaturesCol("features")
  .setK(5)                  // 聚类数（最重要的参数）
  .setMaxIter(50)           // 最大迭代次数
  .setTol(1e-4)             // 收敛阈值
  .setSeed(42)              // 随机种子（保证可复现）
  .setInitMode("k-means||") // 初始化方式：k-means|| / random
  .setInitSteps(2)          // K-Means|| 的步数

val model = kmeans.fit(featuresDF)

// 聚类中心
println(s"Cluster Centers: ${model.clusterCenters.mkString(", ")}")
// 输出每个簇的中心点坐标，可以观察不同簇的特征差异

// 预测
val predictions = model.transform(featuresDF)

// 评估 — 轮廓系数（Silhouette Score）
val evaluator = new ClusteringEvaluator()
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
  .setMetricName("silhouette")

val silhouette = evaluator.evaluate(predictions)
println(s"Silhouette Score: $silhouette")
// 范围 [-1, 1]，越大越好
// > 0.5 表示聚类结构合理
// > 0.7 表示聚类结构良好
// < 0.2 表示聚类结构不明显
```

### 肘部法则选 K

选择 K 值（聚类数）是 K-Means 最核心的问题。肘部法通过观察 SSE（误差平方和）随 K 的变化曲线来找到最佳 K 值：

```scala
// 遍历 K 值，找到 SSE 下降的拐点
val ks = 2 to 10
val costs = ks.map { k =>
  val kmeans = new KMeans()
    .setK(k)
    .setSeed(42)
    .setFeaturesCol("features")

  val model = kmeans.fit(featuresDF)
  val cost = model.summary.trainingCost  // SSE
  val silhouette = evaluator.evaluate(model.transform(featuresDF))
  (k, cost, silhouette)
}

costs.foreach { case (k, cost, sil) =>
  println(s"K=$k, SSE=$cost, Silhouette=$sil")
}

// 观察输出：SSE 下降从快速变缓的那个 K 值就是肘部（拐点）
```

三种选 K 的方法对比：

| 方法 | 原理 | 适用场景 | 缺点 |
|------|------|---------|------|
| 肘部法 | SSE 下降变缓的位置 | 大部分场景 | 拐点可能不明显 |
| 轮廓系数 | 最大化平均轮廓系数 | 数据分布较清晰时 | 计算量大 |
| 业务法 | 根据业务需求确定 | 有明确的业务分类目标 | 可能和数据特性不符 |

> **实际经验**：在实际项目中，肘部法的拐点往往不明显——曲线可能一直平缓下降。这时候我更推荐结合业务法：比如用户分群需要分"高价值、中价值、低价值"3 类，那 K=3 就够了。不要太纠结于"最优"的 K，能配合业务解释的 K 才是好 K。

### K-Means 的局限

```
K-Means 的五大局限：
1. 必须预先指定 K（实际场景 K 通常未知）
2. 对初始中心敏感（不同 seed 可能结果不同）
3. 只能发现球形簇（无法发现月牙形、S 形簇）
4. 对异常值敏感（离群点会拉偏簇中心）
5. 不适合高维数据（维度灾难，距离度量失效）
```

## Bisecting K-Means（二分 K-Means）

Bisecting K-Means 是 K-Means 和层次聚类的结合体。它在"自顶向下"分裂簇的过程中，使用 K-Means 来细化：

```scala
import org.apache.spark.ml.clustering.BisectingKMeans

// 算法思路：
// 1. 初始所有数据为一个簇
// 2. 选择 SSE 最大的簇进行二分
// 3. 重复直到达到 K 个簇

val bkm = new BisectingKMeans()
  .setK(5)
  .setMaxIter(20)
  .setSeed(42)
  .setMinDivisibleClusterSize(1.0)

val model = bkm.fit(featuresDF)
println(s"Cost: ${model.trainingCost}")
```

| 对比维度 | K-Means | Bisecting K-Means |
|---------|---------|-------------------|
| 运行速度 | 快 | 更慢（需要逐步分裂） |
| 初始中心影响 | 大（随机初始化影响大） | 小（层次化稳定） |
| 结果稳定性 | 不稳定 | 更稳定 |
| 适用场景 | 常规聚类 | 需要更稳定结果时 |

## Gaussian Mixture Model（高斯混合模型）

GMM 是 K-Means 的"软聚类"版本。K-Means 把一个点**硬性分配**给最近的簇，GMM 则给每个点一个属于各个簇的**概率**：

```scala
import org.apache.spark.ml.clustering.GaussianMixture

// GMM 假设数据由多个高斯分布混合生成
// K-Means 的"软聚类"版本（每个点属于所有簇，但有概率权重）

val gmm = new GaussianMixture()
  .setK(3)
  .setMaxIter(100)
  .setTol(0.01)
  .setSeed(42)

val model = gmm.fit(featuresDF)

// 每个高斯分布的权重、均值和协方差
model.gaussiansDF.show()

// 预测带有概率
val predictions = model.transform(featuresDF)
predictions.select("features", "prediction", "probability").show(5)
// +----------+----------+-------------------+
// | features |prediction|        probability|
// +----------+----------+-------------------+
// |  [1.2,..]|         0|[0.85,0.10,0.05]  |
// +----------+----------+-------------------+
// 这个点属于簇 0 的概率是 85%，簇 1 是 10%，簇 2 是 5%
```

K-Means vs GMM 的选择：

| 对比维度 | K-Means | GMM |
|---------|---------|-----|
| 簇的形状 | 球形 | 椭圆形（各向异性） |
| 分配方式 | 硬分配（0 或 1） | 软分配（概率） |
| 计算复杂度 | O(nkd) | O(nkd²)（更复杂） |
| 收敛速度 | 快 | 慢（EM 算法迭代） |
| 小样本 | 不稳定 | 可能不收敛 |

## LDA（隐含狄利克雷分配）

LDA 是文本挖掘中的经典算法，用于从大量文档中发现主题结构。比如给定 100 万篇新闻文章，LDA 可以自动发现"体育"、"科技"、"政治"等主题：

```scala
import org.apache.spark.ml.clustering.LDA

// LDA 用于文本主题建模
// 输入：词频向量（CountVectorizer 的输出）

val lda = new LDA()
  .setK(10)                  // 主题数
  .setMaxIter(100)
  .setOptimizer("online")    // online / em
  .setDocConcentration(-1)   // 文档-主题分布的先验（-1 自动设置）
  .setTopicConcentration(-1) // 主题-词分布的先验（-1 自动设置）

val model = lda.fit(corpusDF)

// 查看主题
model.describeTopics(5).show(10, truncate = false)
// +-----+------------------------------+----------------------------+
// |topic|termIndices                   |termWeights                 |
// +-----+------------------------------+----------------------------+
// |0    | [5, 94, 62, 28, 10]          |[0.05, 0.04, 0.03...]      |
// |1    | [23, 7, 1, 45, 67]           |[0.06, 0.04, 0.03...]      |
// +-----+------------------------------+----------------------------+

// 每篇文档的主题分布
val topics = model.transform(corpusDF)
topics.select("topicDistribution").show(5)
// 每个文档属于各个主题的概率分布
```

LDA 的应用场景：

| 场景 | 说明 |
|------|------|
| 新闻聚类 | 将新闻自动归类到不同主题 |
| 用户兴趣建模 | 分析用户阅读/购买的内容主题 |
| 推荐系统 | 基于主题相似度推荐内容 |
| 异常检测 | 主题分布异常的文章可能是垃圾内容 |

## ALS 推荐算法

### 为什么 ALS 是 Spark 最成熟的推荐算法？

ALS（交替最小二乘法）是协同过滤的一种实现，它通过用户-物品的交互历史来预测用户对未见过的物品的偏好。Spark 选择 ALS 而不是 SGD 来做协同过滤，有一个很重要的原因：

**ALS 在交替固定用户矩阵或物品矩阵时，每个用户/物品的计算是独立的——天然适合分布式环境中的并行化。**

```scala
import org.apache.spark.ml.recommendation.ALS

val als = new ALS()
  .setUserCol("userId")
  .setItemCol("movieId")
  .setRatingCol("rating")
  .setRank(10)           // 隐因子数量（越大越精确但越慢）
  .setMaxIter(20)        // 最大迭代次数
  .setRegParam(0.05)     // 正则化参数
  .setAlpha(1.0)         // 隐式反馈的置信度参数
  .setImplicitPrefs(false)  // false=显式评分(1-5)，true=隐式反馈(点击/购买)
  .setColdStartStrategy("drop")  // 预测时处理未知用户/物品

val model = als.fit(trainDF)
```

### ALS 调参

ALS 有四个关键参数需要调优：

```scala
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.evaluation.RegressionEvaluator

val paramGrid = new ParamGridBuilder()
  .addGrid(als.rank, Array(5, 10, 20))
  .addGrid(als.regParam, Array(0.01, 0.05, 0.1))
  .addGrid(als.maxIter, Array(10, 20))
  .build()

val evaluator = new RegressionEvaluator()
  .setMetricName("rmse")
  .setLabelCol("rating")
  .setPredictionCol("prediction")

val cv = new CrossValidator()
  .setEstimator(als)
  .setEvaluator(evaluator)
  .setEstimatorParamMaps(paramGrid)
  .setNumFolds(3)

val cvModel = cv.fit(trainDF)
```

ALS 参数调优指南：

| 参数 | 含义 | 调优建议 |
|------|------|---------|
| `rank` | 隐因子数量 | 5~50，越大越精确但计算量和存储都线性增长 |
| `regParam` | 正则化系数 | 0.01~0.1，越大越防过拟合 |
| `maxIter` | 迭代次数 | 10~20，通常 10 次就够收敛 |
| `alpha` | 隐式反馈置信度 | 10~40（仅隐式反馈时使用），越大表示对正样本信心越足 |

### ALS 推荐

```scala
// 为用户推荐 N 个物品
val userRecs = model.recommendForAllUsers(10)
userRecs.show(5, truncate = false)
// +------+-------------------------------------+
// |userId|recommendations                      |
// +------+-------------------------------------+
// |1     |[{movieId: 120, rating: 4.5}, {...}] |
// |2     |[{movieId: 85, rating: 4.2}, {...}]  |
// +------+-------------------------------------+

// 为物品推荐 N 个用户
val itemRecs = model.recommendForAllItems(10)

// 为指定用户推荐
val someUsers = usersDF.limit(10)
val targetedRecs = model.recommendForUserSubset(someUsers, 10)
```

> **踩坑经验**：`setColdStartStrategy("drop")` 在生产环境中非常重要。默认情况下，如果预测数据中的用户或物品在训练集中从未出现过，ALS 会报错。设置为 "drop" 可以自动过滤掉这些未知的用户/物品，避免任务失败。

### ALS 原理

ALS 将用户-物品评分矩阵分解为两个低秩矩阵的乘积：

```
R(m×n) ≈ U(m×k) × V(n×k)ᵀ

其中：
  R：用户-物品评分矩阵（m 个用户，n 个物品）
  U：用户隐因子矩阵（每个用户对应一个 k 维向量）
  V：物品隐因子矩阵（每个物品对应一个 k 维向量）
  k：隐因子数量（rank）

训练过程（交替最小二乘）：
  步骤 1：固定 U → 优化 V（用最小二乘法解）
  步骤 2：固定 V → 优化 U（用最小二乘法解）
  步骤 3：交替迭代直到收敛

为什么适合分布式？
  固定 U 时，每个物品的优化是独立的 → 可以并行计算
  固定 V 时，每个用户的优化是独立的 → 可以并行计算
```

### 显式反馈 vs 隐式反馈

ALS 可以处理两种类型的用户数据：

| 对比维度 | 显式反馈 | 隐式反馈 |
|---------|---------|---------|
| 数据来源 | 用户主动评分（1~5 星） | 用户行为（点击、购买、浏览） |
| 数据稀疏度 | 很稀疏（大部分用户不评分） | 相对稠密 |
| 负面信号 | 低分就是负面 | 没有负面信号（未点击≠不喜欢） |
| 置信度 | 高（用户主动给出） | 低（行为数据噪声大） |
| ALS 参数 | `implicitPrefs=false` | `implicitPrefs=true`，需要调 alpha |
| 典型场景 | MovieLens 电影评分 | 电商点击数据 |

## 面试高频考点

### Q: K-Means 的 K 怎么选？

肘部法：画 SSE vs K 的曲线，找到下降变缓的拐点。业务法：根据业务需求确定分类数量（如用户分群为高/中/低客单价 3 类）。轮廓系数法：选择平均轮廓系数最大的 K。实际生产中最常用的是**业务法**，因为聚类结果最终要用业务去解释和落地。

### Q: ALS 为什么比 SGD 更适合 Spark？

ALS 在交替固定 U 或 V 时，每个用户/物品的优化是独立的，天然适合并行化。每个 Executor 可以独立处理一部分用户/物品的计算。SGD 需要全局同步更新参数，在分布式环境下通信开销大、收敛慢。这也是为什么 Spark 选择了 ALS 而不是 SGD 作为协同过滤的推荐算法。

### Q: 隐式反馈和显式反馈在 ALS 中的区别？

显式反馈（评分 1-5 星）直接作为优化目标，未观测到的就是缺失值。隐式反馈（点击、浏览、购买）需要把行为量化为置信度，核心问题是没有直接的负面信号——用户没点击某商品不代表他不喜欢。ALS 通过 `alpha` 参数控制正样本的置信度权重。

### Q: GMM 和 K-Means 的区别？

K-Means 是"硬聚类"，每个点只能属于一个簇，且簇的形状必须是球形的。GMM 是"软聚类"，每个点属于所有簇的概率之和为 1，可以拟合椭圆形的簇。GMM 的计算复杂度更高，但能处理更复杂的数据分布。

## 小结

| 算法 | 类型 | 适用场景 |
|------|------|---------|
| K-Means | 聚类 | 用户分群、异常检测 |
| Bisecting K-Means | 聚类 | 层次聚类需求 |
| GMM | 软聚类 | 点属于多个簇的概率建模 |
| LDA | 主题建模 | 文本主题挖掘 |
| ALS | 协同过滤 | 推荐系统、用户-物品预测 |
