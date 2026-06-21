# Spark MLlib — 生产部署与最佳实践

## 模型上线流程

### 为什么模型上线这么重要？

机器学习项目经常被比喻为一个"冰淇凌"——在实验室里看起来完美无缺，到生产环境里就化得一塌糊涂。从开发到生产部署，存在各种"魔鬼细节"：

| 开发环境 | 生产环境 | 常见问题 |
|---------|---------|---------|
| 小数据量（几万条） | 全量数据（几亿条） | 内存溢出、OOM |
| 单机跑 | 分布式集群 | 数据倾斜、Shuffle 瓶颈 |
| 模型训练和预测在一起 | 训练和预测分开 | 特征工程不一致 |
| 数据质量可控 | 线上数据不可控 | 未知类别、缺失值导致报错 |
| 只有少数人用 | 服务大量用户 | 响应时间、吞吐量 |

Spark ML 模型从开发到生产的典型流程应该是这样的：

```
开发环境                                     生产环境
───────────                                 ───────────
训练数据 → 特征工程 → 模型训练 → 评估
                                    ↓
                              save() → PipelineModel
                                    ↓
                            加载模型 → transform(newData) → 预测结果
```

## Pipeline 模型部署

### 保存模型

PipelineModel 的保存是模型部署的第一步。注意：我们保存的是**完整的 Pipeline** 而不是单独的模型，这样做的好处是上线后只需要提供原始数据，Pipeline 内部自动做特征工程：

```scala
// 保存完整 Pipeline（包含所有特征工程步骤和模型）
pipelineModel.write.overwrite().save("hdfs://models/v1/rf_pipeline")

// 保存模型时自动保存的目录结构：
// hdfs://models/v1/rf_pipeline/
//   ├── _SUCCESS
//   ├── metadata/
//   │   ├── part-00000    # Pipeline 元数据
//   │   └── ...
//   ├── stages/
//   │   ├── 0_StringIndexer_xxx/
//   │   │   ├── data/           # 类别到索引的映射表
//   │   │   └── metadata/
//   │   ├── 1_VectorAssembler_xxx/
//   │   │   └── metadata/
//   │   ├── 2_StandardScaler_xxx/
//   │   │   ├── data/           # 均值和标准差
//   │   │   └── metadata/
//   │   └── 3_RandomForestClassificationModel_xxx/
//   │       ├── data/           # 树的结构
//   │       └── metadata/
//   └── ...
```

### 加载模型并预测

生产环境中，加载保存好的模型，对新的原始数据做预测：

```scala
// 生产环境加载
import org.apache.spark.ml.PipelineModel

val model = PipelineModel.load("hdfs://models/v1/rf_pipeline")

// 对新数据做预测（自动执行所有 Pipeline Stage）
val newData = spark.read.parquet("hdfs://data/new_orders")
val predictions = model.transform(newData)

// 保存预测结果
predictions.select("id", "prediction", "probability")
  .write.mode("overwrite")
  .parquet("hdfs://output/predictions")
```

### 模型版本管理

模型版本管理是工业级系统的必备能力。没有版本管理，出了问题无法回滚：

```scala
// 推荐：使用版本目录管理
// hdfs://models/
//   ├── v1/   # 初始版本
//   ├── v2/   # 优化版本
//   └── v3/   # 当前版本

// 代码中使用稳定路径
val modelVersion = "v3"
val modelPath = s"hdfs://models/$modelVersion/rf_pipeline"
```

| 版本管理要素 | 说明 | 实践建议 |
|------------|------|---------|
| 版本号 | v1/v2/v3 或日期 | 结合语义化版本 + 日期 |
| 回滚机制 | 能够快速切回旧版本 | 保留最近 3~5 个版本 |
| 元数据 | 记录训练数据、特征、参数 | 保存到 MySQL 或模型注册表 |
| A/B 测试 | 新版和旧版同时运行 | 监测指标变化后再全量切换 |

## 在线预测 vs 离线预测

### 离线预测（批处理）

这是最常用的模式——适合不需要实时响应的场景（如每日推荐、批量打分）：

```scala
// 每天跑一次，预测全量用户
val todayData = spark.read.parquet("hdfs://data/daily/users_dt=$(date +%Y%m%d)")
val predictions = model.transform(todayData)
predictions.write.mode("overwrite").parquet("hdfs://output/predictions/$(date +%Y%m%d)")

// 然后再将预测结果写入 Redis/MySQL
// Web 服务从 Redis 中读取推荐结果，无需实时调用模型
```

离线预测的典型流程：

| 步骤 | 描述 | 频次 |
|------|------|------|
| 1. 收集新数据 | 从业务库抽取 | 每天/每小时 |
| 2. 特征处理 | Spark SQL 做特征计算 | 每天/每小时 |
| 3. 模型预测 | `model.transform()` 批量预测 | 每天/每小时 |
| 4. 写入缓存 | 结果写入 Redis/MySQL | 每天/每小时 |
| 5. 在线服务 | Web 服务从缓存读取结果 | 实时 |

### 在线预测（单条预测）

Spark ML 模型**不能直接**在 Web 服务中作为 REST API 使用——因为 Spark 每次启动都需要初始化上下文（SparkSession），这需要几秒甚至十几秒的时间。在线预测的实现方式：

```
方案 1：将模型导出为 PMML/PFA 格式
  → 在 Java 端加载，适用于毫秒级预测
  → 缺点：仅部分算法支持，复杂模型可能不支持

方案 2：在 Redis/MySQL 中存储模型参数
  → 应用端读取参数，自行计算预测
  → 适用于 LR 等简单模型（特征维度固定）

方案 3：Spark 批处理 + 缓存（最常用）
  → 预计算所有用户的预测结果，存入 Redis/MySQL
  → Web 服务从缓存读结果，毫秒级响应
  → 缺点：预测结果有时效性，需要定期刷新

方案 4：Structured Streaming 实时预测
  → Kafka → Spark Streaming → 预测 → Redis
  → 秒级延迟，接近实时
```

## 模型监控

### 为什么需要监控？

模型上线后不是一劳永逸的。随着时间的推移，数据分布可能发生变化（Concept Drift），导致模型效果下降：

### 预测结果质量监控

```scala
// 1. 每日统计预测分布
predictions.agg(
  avg("prediction").alias("avg_pred"),
  stddev("prediction").alias("std_pred"),
  count("*").alias("total")
).show()

// 2. 监控特征漂移（Feature Drift）
// 如果特征的分布和训练时发生明显变化，说明数据变了
// 简单做法：比较特征均值和标准差

// 3. 检查异常预测值
predictions.filter($"prediction" < 0 || $"prediction" > 1).count()

// 4. 监控预测结果的三天对比
// 如果某一天预测分布突变，说明可能有问题
```

### 模型监控的关键指标

| 监控维度 | 监控指标 | 告警阈值 |
|---------|---------|---------|
| 数据量 | 输入行数 | 突然下降 50% |
| 预测分布 | 预测值的均值和标准差 | 相比训练集偏差 > 20% |
| 特征分布 | 各特征的均值和方差 | 相比训练集偏差 > 30% |
| 缺失值 | 缺失率 | 超过训练时的 2 倍 |
| 空值 | 空值比例 | > 5% |
| 预测缺失 | 预测结果为 null 的比例 | > 0 |

## MLlib 最佳实践

### 1. 数据预处理

这是 ML Pipeline 中最容易忽略但最重要的环节：

```scala
// 处理缺失值
val cleaned = df.na.fill(0, Array("age"))         // 数值列填 0
  .na.fill("unknown", Array("city"))                // 类别列填 "unknown"
  .na.drop(Array("label"))                          // label 必须非空

// 异常值处理（IQR 方法）
val q1 = df.stat.approxQuantile("amount", Array(0.25), 0.01)(0)
val q3 = df.stat.approxQuantile("amount", Array(0.75), 0.01)(0)
val iqr = q3 - q1
val bounded = df.filter($"amount" >= q1 - 1.5 * iqr && $"amount" <= q3 + 1.5 * iqr)
```

> **踩坑经验**：缺失值处理一定要在 Pipeline 内部完成，而不是在外部手动处理。如果缺失值处理逻辑在 Pipeline 之外，线上预测时很容易忘了做同样的处理，导致 Training-Serving Skew。

### 2. 特征选择

```scala
// 排查冗余特征
// 高相关特征 → 保留一个（避免多重共线性）
val corrMatrix = featuresDF.stat.corr("age", "income")

// 低方差特征 → 删除（信息量少）
// 方差接近 0 的特征对模型贡献小
// 比如"性别"特征中 99% 是男性，这个特征几乎不提供区分度
```

### 3. 训练验证策略

#### 时间序列数据的划分（最常见的失误）

```scala
// 时间序列数据 — 不能用随机划分！
// ❌ 错误做法：随机划分
val Array(train, test) = df.randomSplit(Array(0.8, 0.2))

// ✅ 正确：按时间划分（绝对不能使用未来数据训练）
val train = df.filter($"dt" < "2024-06-01")
val test = df.filter($"dt" >= "2024-06-01")

// 分层抽样 — 保持类别比例（处理不平衡数据时很重要）
val fractions = Map("A" -> 0.8, "B" -> 0.8, "C" -> 0.8)
val stratifiedTrain = df.stat.sampleBy("label", fractions, 42)
```

#### 数据划分的常见陷阱

| 陷阱类型 | 说明 | 解决方案 |
|---------|------|---------|
| 时间穿越 | 用未来的数据预测过去 | 严格按时间划分 |
| 用户重叠 | 同一用户同时出现在训练和测试集 | 按用户 ID 划分 |
| 随机划分 | 时间序列数据的随机划分会导致信息泄露 | 按时间窗口划分 |
| 数据泄露 | 特征中包含 label 的衍生变量 | 仔细审查特征工程逻辑 |

### 4. 资源优化

```scala
// ML 训练的资源建议
// 1. 每个 Executor 4~8 核（避免过多的 GC 开销）
// 2. shuffle.partitions ≈ cores × 2~3
// 3. 交叉验证的并行度 = min(paramGrid, executors)

// 设置 CrossValidator 并行度
cv.setParallelism(4)  // 同时训练 4 个模型

// 缓存频繁使用的中间数据
val cachedFeatures = featureDF.cache()

// 记得在不再需要时释放缓存
cachedFeatures.unpersist()
```

### 5. 常见陷阱

```scala
// 陷阱 1：Label 泄露
// 将未来信息作为特征（如用明天的价格预测今天）
// 检查：特征中是否包含 label 的衍生变量
// 经典案例：用"是否逾期"预测"是否会逾期"——结果训练集 Accuracy 99%，上线后效果一塌糊涂

// 陷阱 2：训练/测试数据重叠
// 重复的 userId 同时出现在训练和测试集
// 解决：按 userId 划分（确保一个 userId 只在一边）

// 陷阱 3：类别不平衡的处理
// 1. 欠采样（Under-sampling）——随机丢弃多数类样本
// 2. 过采样（Over-sampling）——复制少数类样本
// 3. 调整 class weight ——在损失函数中给少数类更高权重
// 4. 使用 AUC 代替 Accuracy 评估

// 陷阱 4：Pipeline 中的缓存问题
// PipelineModel.transform(newData) 是重新执行，不会使用之前 cache
// 每次 transform 都是完整 Pipeline 执行
// 所以如果要对不同时间的数据多次预测，每次都重新读取原始数据

// 陷阱 5：未知类别处理
// 线上数据的类别列可能出现训练集从未见过的值
// Solution：StringIndexer 设置 .setHandleInvalid("keep")
```

### 生产环境部署检查清单

| 检查项 | 检查内容 | 是否通过 |
|--------|---------|---------|
| 特征一致性 | 训练和预测的特征逻辑一致 | □ |
| 缺失值处理 | 所有列都有缺失值处理策略 | □ |
| 未知类别处理 | StringIndexer 设置了 handleInvalid | □ |
| 数据划分 | 时间序列用时间划分，不使用 future data | □ |
| 模型持久化 | PipelineModel 完整保存 | □ |
| 版本管理 | 模型有版本号和回滚方案 | □ |
| 监控告警 | 预测分布异常有告警 | □ |
| 资源评估 | Executor 和内存配置合理 | □ |

## 面试高频考点

### Q: Spark ML 模型如何上线？

1. 将完整的 PipelineModel 保存到 HDFS——包含特征工程和模型
2. 生产环境中用 Spark 批处理或 Structured Streaming 加载模型
3. 调用 `model.transform(newData)` 做预测——只需要传入原始数据
4. 结果写入 Redis/MySQL 供在线服务查询
5. 设置模型版本管理，保留回滚能力

> 面试时可以补充说明：**为什么要保存完整的 PipelineModel 而不是单独的模型？** 因为这样能保证训练和预测时的特征工程逻辑完全一致，避免线上手续费时漏掉某一步。

### Q: 训练数据和预测数据的 Schema 不一致怎么办？

PipelineModel 要求输入数据的 Schema 与训练时一致。特征列必须相同（包括列名和类型）。如果缺少某些列，需要在 transform 之前做数据补全。Spark 的 Pipeline 已经包含特征工程步骤，所以 `transform()` 时只需要提供原始数据列即可，不需要自己再做特征处理。

常见解决方案：
1. 缺失列补默认值（`df.withColumn("missing_col", lit(0))`）
2. 使用 coalesce 处理空值
3. 在 Pipeline 中第一层就做缺失值填充

### Q: 模型更新策略？

1. 离线训练新模型 → 保存到新路径（v2/v3）
2. 在 HDFS 或配置中心切换生产环境加载的路径
3. 观察新模型的效果（A/B 测试）
4. 确认新模型稳定后，保留旧模型用于回滚
5. 出现问题时快速切回旧版本

### Q: 如何处理训练数据和线上数据的分布不一致？

这就是机器学习中常说的"数据漂移（Data Drift）"问题。解决方案：
1. **监控特征分布**：定期比较训练数据和线上数据的特征分布
2. **定期重训练**：设置定时任务，用最新数据重新训练模型
3. **异常检测**：当特征分布发生显著变化时触发告警
4. **数据质量检查**：在数据进入 Pipeline 之前做数据质量校验

## 小结

| 阶段 | 要点 |
|------|------|
| 模型保存 | PipelineModel.write.save，包含全部特征工程步骤 |
| 模型加载 | PipelineModel.load，输出 DataFrame |
| 离线预测 | Spark 批处理，每天/每小时跑一次 |
| 在线预测 | 导出模型参数 or 预计算结果到缓存 |
| 版本管理 | v1/v2/v3 目录，原子切换，支持回滚 |
| 模型监控 | 预测分布、特征分布、缺失值监控 |
| 数据陷阱 | Label 泄露、时间穿越、数据重复、未知类别 |
| 资源优化 | Executor 4~8 核，合理设置并行度 |
