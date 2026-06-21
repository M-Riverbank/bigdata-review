# Spark MLlib — 特征工程

## 特征工程总览

### 为什么说特征工程比模型更重要？

有句话在机器学习圈广为流传：**"数据和特征决定了机器学习的上限，而模型和算法只是逼近这个上限。"** 这句话到底是什么意思？

简单说，如果你的特征质量差（信息量不足、噪声太大、量纲不统一），再好的算法也救不回来。反过来，如果你的特征工程做得好，即使只用逻辑回归这种最简单的模型，效果也可能超过用了复杂模型但特征工程敷衍了事的方案。

在实际的工业项目中，数据科学家和机器学习工程师花在特征工程上的时间通常占到整个项目周期的 **60% ~ 80%**。模型选型和调参反而只占一小部分。

```
特征工程流程：
原始数据 → 清洗（缺失值、异常值） → 编码（类别转数值） → 变换（缩放/降维） → 选择（筛选有效特征） → 训练特征
```

### MLlib 特征工程的全景图

Spark MLlib 提供了一整套特征工程的工具，覆盖了从原始数据到模型输入的完整链路：

| 阶段 | 目的 | 常用 Transformer |
|------|------|-----------------|
| 编码 | 类别文本转数值 | StringIndexer, OneHotEncoder, Bucketizer |
| 变换 | 统一量纲、降维 | StandardScaler, MinMaxScaler, PCA, Normalizer |
| 组装 | 多列合并为特征向量 | VectorAssembler |
| 选择 | 筛选有效特征 | ChiSqSelector, VectorSlicer |
| 文本 | 文本转数值特征 | Tokenizer, HashingTF, CountVectorizer, IDF |

## 特征编码

### StringIndexer — 字符串索引化

这是特征编码中最基础的一步。假设你的数据中有个 `city` 列，值是"北京""上海""广州"这类字符串，模型是看不懂的。StringIndexer 会把它们映射成数值索引：

```scala
import org.apache.spark.ml.feature.StringIndexer

val indexer = new StringIndexer()
  .setInputCol("city")
  .setOutputCol("cityIdx")
  .setHandleInvalid("keep")  // 未知值：keep / skip / error

val indexed = indexer.fit(df).transform(df)
// 按频率排序："北京"（出现最多）→ 0.0, "上海" → 1.0, "广州" → 2.0
```

StringIndexer 的几个关键参数：

| 参数 | 默认值 | 说明 | 推荐设置 |
|------|-------|------|---------|
| `handleInvalid` | "error" | 遇到未知值的处理方式 | 生产环境用 "keep" |
| 排序方式 | 按频率降序 | 出现最多的类别索引为 0 | 通常是业务上想要的 |
| StringOrderType | frequencyDesc | 可改为 alphabetDesc 等 | 大部分场景不用改 |

> **踩坑经验**：`setHandleInvalid("keep")` 是生产环境中必须设置的一个参数。默认是 `"error"`，意思是如果预测数据中出现了一个训练集里没有的类别（比如训练时只有"北京""上海"，线上来了个"深圳"），任务会直接抛异常。在线上环境中，新类别是常态，用 "keep" 可以优雅处理——将未知值映射为最后一个索引值。

### OneHotEncoder — 独热编码

StringIndexer 虽然把文字转成了数字，但这里引入了一个问题：**数值大小隐含了顺序关系**。"北京"=0，"上海"=1，"广州"=2，模型会认为"广州 > 上海 > 北京"。这在大部分场景下是毫无意义的。

OneHotEncoder 就是为了消除这个顺序假设而生的：

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

独热编码的效果对比：

| 编码方式 | 北京 | 上海 | 广州 | 问题 |
|---------|------|------|------|------|
| 直接数值 | 0 | 1 | 2 | 隐含 0 < 1 < 2 的顺序 |
| OneHot 不丢弃 | [1,0,0] | [0,1,0] | [0,0,1] | 三个特征存在线性相关 |
| OneHot 丢弃最后一个 | [1,0] | [0,1] | [0,0] | 消除冗余，K-1 个特征即可 |

> **面试点**：`setDropLast(true)` 是为了解决"虚拟变量陷阱"（Dummy Variable Trap）。对于 K 个类别，只需要 K-1 个二元特征就能完全表达——如果 K 个都保留，线性模型中的截距项会导致多重共线性。但需要注意：树模型（RF, GBDT）不受多重共线性影响，可以设置为 false。

### Bucketizer — 连续值离散化

有些场景下，连续数值特征（如年龄、收入）按分桶处理效果更好。比如年龄对某些疾病的影响不是线性的，而是有阶段性的：

```scala
import org.apache.spark.ml.feature.Bucketizer

// 定义分桶边界——注意边界个数 = 分段数 + 1
val splits = Array(Double.NegativeInfinity, 0, 18, 35, 60, Double.PositiveInfinity)

val bucketizer = new Bucketizer()
  .setInputCol("age")
  .setOutputCol("ageGroup")
  .setSplits(splits)
// 年龄 → 分组：(-∞, 0) → 0, [0, 18) → 1, [18, 35) → 2, [35, 60) → 3, [60, +∞) → 4
// 实际上 [-∞, 0) 这个分组应该不会出现，但为了完整性写上了
```

分桶边界的设定方式：

| 方法 | 说明 | 适用场景 |
|------|------|---------|
| 等距分桶 | 均匀划分，如 0-20,20-40,40-60 | 年龄、时间等均匀分布 |
| 等频分桶 | 根据分位数，每个桶样本量相同 | 长尾分布的特征 |
| 业务分桶 | 根据业务逻辑，如未成年/青年/中年/老年 | 有明确业务含义 |

## 特征变换

### VectorAssembler — 特征向量组装

**这是整个 Spark MLlib 中使用频率最高的 Transformer**。因为所有 ML 算法都要求输入是一个 `Vector` 类型的列，而不是多个分散的数值列。你需要用 VectorAssembler 把多列合成一列：

```scala
import org.apache.spark.ml.feature.VectorAssembler

val assembler = new VectorAssembler()
  .setInputCols(Array("age", "income", "cityVec", "educationIdx"))
  .setOutputCol("features")
// 将多个数值列合并为一个特征向量
```

VectorAssembler 的输入列可以是：
- 数值类型列（int, long, float, double）
- Vector 类型列（如 OneHotEncoder 的输出 `cityVec`）
- 但不能是 String 类型！

所以在 Pipeline 中的顺序通常是：`StringIndexer → OneHotEncoder → VectorAssembler → ...`

### StandardScaler — 标准化

想象一下你的数据中有两个特征：年龄（0~100）和收入（0~100 万）。如果不做标准化，基于距离的算法（KNN、K-Means、SVM）和基于梯度的算法（LR、神经网络）会**天然偏向收入这个特征**，因为它的数值波动范围大。

```scala
import org.apache.spark.ml.feature.StandardScaler

val scaler = new StandardScaler()
  .setInputCol("features")
  .setOutputCol("scaledFeatures")
  .setWithStd(true)   // 除以标准差，使方差为 1（默认 true）
  .setWithMean(true)  // 减去均值，使数据中心化（只支持稠密向量，默认 false）

// 标准化后的特征：均值为 0，标准差为 1
// z = (x - μ) / σ
```

哪些算法需要标准化？这是一个常考的面试知识点：

| 算法 | 需要标准化吗？ | 原因 |
|------|--------------|------|
| 线性回归 / 逻辑回归 | **必须** | 梯度下降需要各维度尺度一致，否则收敛慢 |
| SVM | **必须** | 对特征尺度敏感 |
| K-Means / KNN | **必须** | 基于距离计算，量纲大的特征主导结果 |
| PCA | **必须** | 方差大的维度权重大 |
| 决策树 / 随机森林 | **不需要** | 基于阈值分裂，不受特征尺度影响 |
| 朴素贝叶斯 | **视情况** | 高斯朴素贝叶斯需要，多伯努利不需要 |

> **踩坑经验**：`setWithMean(true)` 对稀疏数据是危险的！稀疏数据中大多数元素是 0，减去均值后变成负数，破坏了稀疏性——原本只存储非零元素的稀疏向量会变成全稠密向量，内存爆炸。所以对稀疏高维特征（特别是文本 TF-IDF），**禁用** `setWithMean(true)`。

### MinMaxScaler — 归一化

```scala
import org.apache.spark.ml.feature.MinMaxScaler

val minMax = new MinMaxScaler()
  .setInputCol("features")
  .setOutputCol("normalizedFeatures")
  .setMin(0.0)
  .setMax(1.0)
// 缩放到 [0, 1] 区间：x' = (x - min) / (max - min) * (max - min) + min
```

StandardScaler vs MinMaxScaler 怎么选？

| 对比维度 | StandardScaler | MinMaxScaler |
|---------|---------------|-------------|
| 输出范围 | 均值 0，标准差 1 | 固定 [0, 1] 或 [-1, 1] |
| 对异常值的稳定性 | 较强（用标准差，受异常值影响较小） | 较弱（min/max 被异常值拉偏） |
| 稀疏数据兼容 | 可以（只用 withStd） | 可以 |
| 适用场景 | 数据有异常值时首选 | 已知 min/max 范围、数据分布均匀 |

### MaxAbsScaler — 绝对值归一化

```scala
import org.apache.spark.ml.feature.MaxAbsScaler

val maxAbs = new MaxAbsScaler()
  .setInputCol("features")
  .setOutputCol("maxAbsFeatures")
// 缩放到 [-1, 1]，0 映射后仍然是 0，不会破坏稀疏性
```

MaxAbsScaler 和 StandardScaler 的一个重要区别：

| 特性 | StandardScaler | MaxAbsScaler |
|------|---------------|-------------|
| 稀疏性保留 | withMean=true 时会破坏 | 始终保留（0→0） |
| 缩放范围 | 无固定范围 | [-1, 1] |
| 适用场景 | 一般标准化场景 | 稀疏数据 + 需要保留稀疏性 |

### Normalizer（范数归一化）

```scala
import org.apache.spark.ml.feature.Normalizer

// 将每个样本的向量缩放到单位范数（按行归一化）
val normalizer = new Normalizer()
  .setInputCol("features")
  .setOutputCol("normFeatures")
  .setP(2.0)  // p=1: L1 范数, p=2: L2 范数（默认）, p=∞: 无穷范数

// 常用于文本分类（每个文档的 L2 范数归一化后，余弦相似度等价于点积）
```

## 特征选择

### ChiSqSelector — 卡方检验

在特征非常多的情况下（比如文本分类中 Token 数量 10 万+），需要用统计学方法筛选出和标签最相关的特征：

```scala
import org.apache.spark.ml.feature.ChiSqSelector

val selector = new ChiSqSelector()
  .setFeaturesCol("features")
  .setLabelCol("label")
  .setNumTopFeatures(50)  // 选择与 label 最相关的 50 个特征
  .setOutputCol("selectedFeatures")
// 卡方值越大 → 特征与 label 相关性越强 → 越值得保留
```

特征选择方法对比：

| 方法 | 原理 | 适用场景 | 优缺点 |
|------|------|---------|--------|
| ChiSqSelector | 卡方检验 | 分类问题，特征和标签都是离散值 | 简单快速，但只检测线性相关 |
| VectorSlicer | 按索引手动选择 | 已知哪些特征重要 | 依赖领域知识 |
| RF 特征重要性 | 随机森林训练后输出 | 所有场景 | 效果好但需要先训练 |

### VectorSlicer — 按索引选择

```scala
import org.apache.spark.ml.feature.VectorSlicer

val slicer = new VectorSlicer()
  .setInputCol("features")
  .setOutputCol("slicedFeatures")
  .setIndices(Array(0, 2, 5))  // 只保留索引 0,2,5 的特征
// 从特征向量中提取指定的几个维度
```

## 文本特征提取

### Tokenizer — 分词

文本处理的起点：把句子拆成单词：

```scala
import org.apache.spark.ml.feature.Tokenizer

val tokenizer = new Tokenizer()
  .setInputCol("text")
  .setOutputCol("words")
// "Hello Spark" → ["hello", "spark"]

// 如果要保留一些特殊字符，可以用 RegexTokenizer
import org.apache.spark.ml.feature.RegexTokenizer

val regexTokenizer = new RegexTokenizer()
  .setInputCol("text")
  .setOutputCol("words")
  .setPattern("\\W+")  // 按非单词字符分词
  .setToLowercase(true)  // 转小写
// "Hello, Spark!" → ["hello", "spark"]
```

### HashingTF — 哈希词频

**不需要维护词汇表**，直接用哈希函数把词映射到固定维度：

```scala
import org.apache.spark.ml.feature.HashingTF

val hashingTF = new HashingTF()
  .setInputCol("words")
  .setOutputCol("rawFeatures")
  .setNumFeatures(10000)  // 特征维度，默认为 2^18 = 262144
// 使用哈希技巧，不需要维护词汇表
// 缺点：不同词可能哈希到同一个桶（哈希碰撞）
// 优点：内存占用固定，适合大规模分布式环境
```

### CountVectorizer — 基于词频

和 HashingTF 的区别在于：需要维护词汇表，计算精确的词频：

```scala
import org.apache.spark.ml.feature.CountVectorizer

val cv = new CountVectorizer()
  .setInputCol("words")
  .setOutputCol("features")
  .setVocabSize(5000)   // 最大词汇量（取最频繁的 5000 个词）
  .setMinDF(5)           // 最少在 5 篇文档中出现
// 保留词汇表，可解释性强
// 注意：fit() 时需要扫描全量数据构建词汇表
```

HashingTF vs CountVectorizer 的选择：

| 对比维度 | HashingTF | CountVectorizer |
|---------|----------|----------------|
| 可解释性 | 差（哈希后不知道对应什么词） | 强（保留词汇表） |
| 内存占用 | 固定（numFeatures 决定） | 与词汇表大小成正比 |
| 碰撞风险 | 有（哈希碰撞） | 无 |
| 分布式扩展 | 好（不需要全局词汇表） | 需要一次全局扫描建词汇表 |
| 适用场景 | 海量数据，不关注具体词是什么 | 需要知道哪些词重要 |

### IDF — 逆文档频率

TF-IDF 的核心思想：**一个词在越少的文档中出现，它就越能区分文档**。比如"的"在每个文档中都出现，信息量为 0；而"机器学习"只在少数文档中出现，信息量很大。

```scala
import org.apache.spark.ml.feature.{HashingTF, IDF}

// TF-IDF = 词频 × 逆文档频率
// 对高频但信息量低的词降权（如"的"、"是"）

val tf = new HashingTF()
  .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(10000)

val idf = new IDF()
  .setInputCol("rawFeatures").setOutputCol("features")
  .setMinDocFreq(5)  // 只在至少 5 篇文档中出现的词才计入

// 完整的文本处理 Pipeline
val pipeline = new Pipeline().setStages(Array(tf, idf))
```

## PCA（主成分分析）

PCA 是最常用的无监督降维方法，它找到数据中方差最大的方向（主成分），将高维数据投影到低维空间：

```scala
import org.apache.spark.ml.feature.PCA

// PCA 降维：减少特征数量，保留主要信息
val pca = new PCA()
  .setInputCol("features")
  .setOutputCol("pcaFeatures")
  .setK(10)  // 降到 10 维
  .fit(df)

println(s"Explained variance: ${pca.explainedVariance}")
// 输出每个主成分解释的方差比例
// 前几个主成分通常能解释大部分方差

val pcaDF = pca.transform(df)
// pcaDF 的 pcaFeatures 列只有 10 维
// 保留了原始特征中方差最大的 10 个主成分
```

PCA 降维前后的对比：

| 维度 | 原始空间 | PCA 降维后 |
|------|---------|-----------|
| 特征数量 | 1000 维 | 10 维 |
| 可解释性 | 好（每个特征有实际含义） | 差（主成分是线性组合） |
| 信息保留 | 100% | 大部分（取决于 K 的选择） |
| 计算复杂度 | 高 | 低 |
| 过拟合风险 | 高 | 低 |

> **面试点**：PCA 主要用于三个场景：1）高维数据降维后训练（减少过拟合）；2）降到 2/3 维做可视化；3）去噪（丢弃方差小的成分）。但要注意 PCA 是无监督的——它不关心 label，所以有可能丢掉和 label 相关但方差小的特征。

## 面试高频考点

### Q: 为什么 StandardScaler 要分别设置 withStd 和 withMean？

`withStd=true`：除以标准差，让数据具有单位方差。`withMean=true`：减去均值，让数据中心化。两者可以独立设置。对于稀疏数据，`setWithMean(true)` 会破坏稀疏性（0 减去均值后变成负数，元素不再稀疏），所以默认关闭。如果你的数据是稠密的，可以同时开启。

### Q: StringIndexer 和 OneHotEncoder 为什么要配合使用？

StringIndexer 把城市名转为数值（0/1/2）。但直接作为数值特征隐含了顺序关系（0 < 1 < 2）。OneHotEncoder 把 0/1/2 转成独热向量，消除顺序假设。StringIndexer 负责"字符串→数值"的映射，OneHotEncoder 负责"数值→无顺序向量"的进一步转换。树模型可以只用 StringIndexer（因为它不怕顺序假设），但线性模型（LR、SVM）必须用 OneHotEncoder。

### Q: PCA 在 MLlib 中怎么用？降维后特征怎么解释？

PCA 是无监督降维，`setK(10)` 降到 10 维。但降维后的每个主成分是原始特征的线性组合。在实际工作中，我们通常不去解释主成分的语义（因为太困难了），而是把它当作一个黑盒降维工具。PCA 更多用于可视化（降到 2/3 维）和去噪。

### Q: VectorAssembler 为什么是 Pipeline 中必不可少的组件？

因为 MLlib 的所有算法都要求输入是 `Vector` 类型的列，不能是多个分散的数值列。VectorAssembler 把多个数值列合并为一个向量，是连接"特征处理"和"模型训练"的桥梁。在 Pipeline 中，它通常位于所有特征 Transformer 之后，Estimator 之前。

## 小结

| 步骤 | 常用方法 | 说明 |
|------|---------|------|
| 类别编码 | StringIndexer + OneHotEncoder | 消除类别顺序假设 |
| 连续值标准化 | StandardScaler | 统一量纲，加速梯度收敛 |
| 连续值归一化 | MinMaxScaler | 缩放到 [0,1] |
| 连续值离散化 | Bucketizer | 年龄分段、收入分层 |
| 特征组装 | VectorAssembler | 合并多个列为特征向量 |
| 文本特征 | HashingTF / CountVectorizer + IDF | 文本转数值 |
| 降维 | PCA | 减少特征数量、去噪 |
| 特征选择 | ChiSqSelector | 筛选与标签相关的特征 |
