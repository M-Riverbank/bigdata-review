# Spark Core — 部署模式与 Spark 3.x 新特性

## 部署模式

在实际工作中，选择哪种部署模式直接影响你的 Spark 作业的稳定性、资源利用率和运维成本。很多新手一开始只会在本地跑 `local[*]`，但到了生产环境就懵了——YARN 和 K8s 到底怎么选？Standalone 是不是已经过时了？本节帮你一次性理清楚。

Spark 支持多种集群管理器（Cluster Manager），每种都有其特定的适用场景：

```scala
// 部署模式对比：
// Local     — 本地单机调试（开发测试）
// Standalone — Spark 自带的集群管理器
// YARN      — Hadoop 资源管理器（生产主流）
// Kubernetes — 容器化部署（趋势）
```

> **面试点**：面试官常问"你们公司 Spark 用的什么部署模式？为什么？"——不要只说"YARN"，要能说出选型理由。

### Local 模式

Local 模式是最简单的部署方式，所有进程都在一个 JVM 中运行。它不是一个真正的集群模式，但对于开发和调试来说非常方便。

```bash
# 所有进程在一个 JVM 中运行
spark-shell --master local[*]         # 使用所有可用线程
spark-shell --master local[4]         # 使用 4 个线程
spark-shell --master local            # 1 个线程
```

这里有个常见误区：`local[*]` 不等于"分布式"，它只是在本地用多线程模拟并行。每个线程相当于一个 Task，但所有 Task 共享同一个 JVM 堆内存。

**适用场景：**
- 本地开发调试
- 单元测试
- 小数据量（MB 级别）逻辑验证
- IDE 中直接运行

**踩坑经验：**
- 用 local 模式测通过的代码，到集群上未必能跑——网络序列化、数据倾斜等问题在 local 模式下很难暴露
- `local[4]` 时如果数据量超过内存，会直接 OOM，不像集群模式会 spill 到磁盘
- 别把 `local` 模式用于性能测试，结果毫无参考价值

### Standalone 模式

Standalone 是 Spark 自带的轻量级集群管理器，不需要依赖任何外部系统。

```bash
# 启动 Master
./sbin/start-master.sh
# Master 默认在 http://host:8080

# 启动 Worker（指向 Master）
./sbin/start-worker.sh spark://host:7077 -c 4 -m 8g

# 提交应用
spark-submit --master spark://host:7077 --class MyApp my.jar
```

**Standalone 的优缺点：**

| 维度 | 说明 |
|------|------|
| 优点 | 部署简单，不需要 Hadoop/YARN 环境，适合小团队 |
| 缺点 | 没有多租户隔离，资源管理能力弱，缺乏完善的安全机制 |
| 适用场景 | 小集群（< 20 节点），测试环境，没有 Hadoop 体系的团队 |

> **面试点**：Standalone 模式下 Master 有单点故障问题吗？——可以配置 Zookeeper 实现 Master HA。

### YARN 模式（生产主流）

YARN 是生产环境中最主流的 Spark 部署方式。如果你的公司已经有 Hadoop 集群，那么 YARN 几乎是必然选择。

```bash
# YARN 两种 deploy-mode
# client — Driver 运行在提交机器（调试用）
# cluster — Driver 运行在 YARN 容器中（生产用）

spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --queue root.queue \
  --num-executors 100 \
  --executor-memory 8g \
  --executor-cores 4 \
  --class com.example.MyApp \
  my-app.jar
```

**YARN client vs cluster 模式对比：**

| 对比项 | client | cluster |
|--------|--------|---------|
| Driver 位置 | 提交机器 | YARN 容器内 |
| 日志查看 | 直接 stdout | 需要通过 `yarn logs` 命令 |
| 故障恢复 | 进程退出即失败 | YARN 自动重启 |
| 适用场景 | 开发调试、交互式 | 生产作业 |
| 网络依赖 | 提交机需连通 Executor | 仅集群内通信 |

**踩坑经验：**
- `--num-executors` 并不是越多越好——要考虑 YARN 队列的资源上限
- Executor 内存设置过大可能导致 YARN container 分配失败
- `--queue` 指定队列，不指定会提交到 default 队列，可能被其他人挤占资源
- Driver 的 OOM 经常被忽略——client 模式看本地内存，cluster 模式要额外为 Driver 申请内存

### Kubernetes 模式（趋势）

K8s 是近几年的热点，越来越多的公司开始将 Spark 作业容器化部署。

```bash
# 需要预先构建 Spark Docker 镜像
./bin/docker-image-tool.sh -t my-tag build

# 提交到 K8s 集群
spark-submit \
  --master k8s://https://k8s-api:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=spark:my-tag \
  --conf spark.kubernetes.namespace=spark-jobs \
  --class com.example.MyApp \
  my-app.jar
```

**K8s 模式的优势：**
- 资源隔离粒度更细——每个 Executor 是一个独立的 Pod
- 弹性伸缩能力更强——结合 K8s HPA 自动扩缩容
- 与微服务体系无缝集成——日志、监控、调度统一管理

**K8s 模式的挑战：**
- 网络性能开销——Pod 之间的网络通信比 YARN 的 NM 通信多一层
- 排错困难——Pod 重启后日志丢失，需要集中式日志收集（如 ELK）
- Shuffle 数据存储——需要挂载共享存储或使用 RSS（Remote Shuffle Service）

> **面试点**：Spark on K8s 和 Spark on YARN 你选哪个？这是一个开放问题，考察你对两种架构的理解。常见的回答思路：已有 Hadoop 生态选 YARN，从零搭建且团队有 K8s 运维能力选 K8s。

### 部署模式对比

| 维度 | Local | Standalone | YARN | Kubernetes |
|------|-------|-----------|------|-----------|
| 部署复杂度 | 无 | 低 | 中 | 高 |
| 资源管理 | 本地 | 内置 | YARN | K8s |
| 多租户 | 否 | 基本 | 完善 | 完善 |
| 动态资源 | 不支持 | 支持 | 支持 | 支持 |
| 生产使用 | 不适用 | 小集群 | **主流** | 快速增长 |
| 学习成本 | 无 | 低 | 中 | 高 |
| 运维成本 | 无 | 低 | 中 | 高 |
| 弹性能力 | 无 | 一般 | 强 | 非常强 |

## Spark 3.x 新特性

Spark 3.x 系列是一个里程碑式的版本。从 3.0 的 AQE 到 3.4 的 Spark Connect，每一个版本都带来了颠覆性的改变。本节我们逐一拆解这些特性，弄明白"它是什么"、"解决了什么问题"、"怎么用"。

### 自适应查询执行（AQE）

AQE（Adaptive Query Execution）是 Spark 3.0 最重磅的特性——它让 Spark 能够在运行时根据实际数据分布动态优化查询计划。在 3.0 之前，Spark 的查询计划是"静态"的：生成计划 -> 执行 -> 结束，中间不会根据实际数据做调整。这导致了一个很尴尬的问题：`spark.sql.shuffle.partitions=200` 这个参数，无论数据是 1MB 还是 1TB，都用 200 个分区。

```scala
// AQE 三大核心优化：
spark.conf.set("spark.sql.adaptive.enabled", "true")  // 默认 3.2+
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

**优化 1：动态合并 Shuffle 分区**

这是 AQE 最直观的优化。Shuffle 之后，Spark 会统计每个分区的数据量。如果分区数据量远小于预期，就说明分区数设置太多了，AQE 会自动合并小分区。

```scala
// 原来：shuffle.partitions=200，每个 Task 处理 5MB
// AQE：自动合并小分区，减少 Task 数量，降低调度开销
```

看一个具体例子：

```scala
// 假设有一张表 1GB，shuffle.partitions=200
// 没有 AQE：200 个 Task，每个处理 ~5MB
// 有 AQE 时：自动合并成 ~20 个 Task，每个处理 ~50MB（接近最优大小）

spark.conf.set("spark.sql.adaptive.coalescePartitions.parallelismFirst", "false")
// 设为 false 时优先合并到目标大小，而非保持并行度
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")
// 目标分区大小，默认 64MB
```

**优化 2：动态调整 Join 策略**

Spark 的 Join 策略有 SortMergeJoin（SMJ）和 BroadcastHashJoin（BHJ）。BHJ 把一个小表广播到所有 Executor，避免 Shuffle，性能远优于 SMJ。但问题在于——优化器在生成计划时，统计信息可能不准（尤其是没做 ANALYZE TABLE 的情况），导致"以为小表很大，用了 SMJ"。

```scala
// 运行时发现小表可广播 → 自动转为 BroadcastHashJoin
// 不需要手动写 broadcast hint

// 手动 hint 的方式（AOE 时代不需要了）：
// import org.apache.spark.sql.functions.broadcast
// result = fact.join(broadcast(dim), "key")
```

**优化 3：动态处理倾斜 Join**

数据倾斜是 Spark 作业中最常见也最头疼的问题。一个 10 亿行的表，某个 key 占了 1 亿行，这个 Task 要跑 1 小时，其他 199 个 Task 几秒就跑完了——这就是典型的"长尾问题"。

```scala
// 自动检测倾斜分区 → 拆分为多个 Task 并行处理
// 默认规则：分区大小 > 中位数 * 5 且 > 256MB

// 相关参数：
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")
```

**AQE 的局限性：**

| 限制 | 说明 |
|------|------|
| 仅作用于 Shuffle 之后 | AQE 的优化时机是 Shuffle 完成后，对 Shuffle 之前的阶段无效 |
| 需要 Shuffle 存在 | 如果整个查询没有 Shuffle，AQE 不会生效 |
| 有一定的性能开销 | 收集运行时统计信息需要额外的计算和网络开销 |

> **面试点**：AQE 为什么选择在 Shuffle 之后做优化？因为 Shuffle 是一个"天然断点"——数据被重新分区写入磁盘，Spark 可以趁这个时机统计每个分区的数据量，然后决定下一步怎么走。这是 AQE 的架构核心。

**踩坑经验：**
- AQE 在 Spark 3.2 之后默认开启，如果你升级了 Spark 版本但作业变慢了，别急着关 AQE——先检查是不是 AQE 的合并逻辑导致了并行度不足
- `spark.sql.adaptive.coalescePartitions.parallelismFirst` 设为 true 时，AQE 会倾向于保持高并行度，可能导致分区合并不够激进
- 对于 already optimized 的查询（手动设置了合理分区数），AQE 的优化空间有限

### 动态分区裁剪（Dynamic Partition Pruning）

动态分区裁剪（DPP）是 Spark 3.0 的另一个"零配置"优化特性。它能在运行时自动进行分区过滤，减少数据扫描量。

```scala
// 3.0+ 自动优化，无需配置
// 原理：从过滤条件中提取分区信息，提前过滤不需要的分区
val fact = spark.read.parquet("fact_table")
val dim = spark.read.parquet("dim_date")

val result = fact.join(dim, "date_id")
  .filter(dim("year") === 2024)
// 自动将 year=2024 推送到 fact_table 的分区裁剪
```

**DPP 的工作原理：**

没有 DPP 时，上面的查询会这样执行：
1. 扫描 `fact_table` **全表**（假设有 5 年的数据，即 1825 个分区）
2. 和 `dim_date` 做 Join
3. 最后过滤出 `year=2024` 的数据

有 DPP 时，执行计划变成：
1. 先扫描 `dim_date`，找到 `year=2024` 对应的 `date_id` 集合
2. 把这个集合作为过滤条件，**只扫描 `fact_table` 中匹配的分区**
3. 再和 `dim_date` 做 Join

**DPP 的适用条件：**

| 条件 | 要求 |
|------|------|
| 表类型 | 必须是分区表（如 `PARTITIONED BY (date_id)`） |
| Join 条件 | Join key 必须包含分区字段 |
| 数据源 | 支持文件级过滤（Parquet、ORC 等列式存储） |
| 过滤条件 | 过滤条件在维度表（小表）侧 |

> **面试点**：DPP 和 AQE 有什么关系？DPP 是**编译时**优化（从逻辑计划推导），AQE 是**运行时**优化（基于实际数据统计），两者是互补关系，可以同时开启。

### Pandas UDAF（Vectorized UDF）

在 Spark 3.0 之前，UDF（User Defined Function）的性能一直是个痛点——每行数据都要经过 JVM <-> Python 的序列化/反序列化，性能损失很大。Pandas UDAF 通过向量化执行（一次处理一批数据），将性能提升了约 10 倍。

```scala
// Spark 3.0 支持 Pandas UDAF，性能比普通 UDF 提升 ~10x
// 需要安装 PyArrow
```

```python
# Python 示例：
from pyspark.sql.functions import pandas_udf
import pandas as pd
import numpy as np

@pandas_udf("double")
def weighted_mean(v: pd.Series, w: pd.Series) -> float:
    return np.average(v, weights=w)

# 使用
df.groupBy("category").agg(weighted_mean(df("value"), df("weight")))
```

**三种 Pandas UDF 类型对比：**

| 类型 | 输入 | 输出 | 适用场景 |
|------|------|------|----------|
| SCALAR | 多列 Series | 一列 Series（相同长度） | 逐行转换，类似 map 操作 |
| GROUPED_MAP | DataFrame | DataFrame | groupBy 后每组输出一个 DataFrame |
| GROUPED_AGG | 多列 Series | 一个标量值 | 自定义聚合函数 |

**踩坑经验：**
- Pandas UDF 需要 PyArrow 库，别忘了安装：`pip install pyarrow`
- 虽然性能优于普通 UDF，但仍有序列化开销——能用内置函数就别用 UDF
- GROUPED_MAP 类型要求输入输出的 schema 保持一致，否则会报错

> **面试点**：为什么 Pandas UDF 比普通 Python UDF 快？核心原因有两点：(1) 向量化执行——一次处理一批数据，减少了函数调用次数；(2) 使用 Apache Arrow 作为序列化格式，避免了逐行序列化的开销。

### 其他重要新特性

这一节把 Spark 3.x 系列中其他值得关注的新特性做个汇总。

**1. ANSI SQL 支持**

```scala
// 1. 更丰富的 ANSI SQL 支持
spark.conf.set("spark.sql.ansi.enabled", "true")
// 开启后：类型不匹配直接报错（不自动转换）
```

ANSI 模式改变了 Spark SQL 的容错行为。默认情况下，Spark 对类型不匹配非常宽容——比如 `'abc' + 1` 会返回 `null`，而不是报错。开启 ANSI 模式后，这类操作会直接抛出异常。

```scala
// 默认模式（非 ANSI）：
// SELECT 'abc' + 1  → 返回 null（静默失败）

// ANSI 模式：
// SELECT 'abc' + 1  → 抛出异常（及时发现 bug）

// 也影响：
// - 除零错误：默认返回 null，ANSI 模式报错
// - 日期解析：默认返回 null，ANSI 模式报错
```

> **面试点**：ANSI 模式是开还是不开？我的建议是——开发阶段开启，尽早发现类型问题；生产环境根据作业的重要性决定，关键作业建议开启。

**2. 新的 UI 页面**

Spark 3.0 在 Web UI 上做了大量改进：

```scala
// - Structured Streaming 监控页
//   可以看到每个 Streaming Query 的输入速率、处理速率、延迟数据等
// - Executor 页新增 线程转储 和 指标
//   线程转储非常有用——当 Executor 卡住时，可以看到它在执行什么代码
```

**3. 内置 GPU 加速（RAPIDS Accelerator）**

```scala
// - 利用 GPU 加速 ETL 和 ML 训练
// - 需要 NVIDIA GPU + RAPIDS Accelerator for Apache Spark

// 配置示例：
// --conf spark.plugins=com.nvidia.spark.SQLPlugin
// --conf spark.rapids.sql.enabled=true
// --conf spark.rapids.memory.gpu.pooling.enabled=false
```

GPU 加速的效果非常显著——某些 ETL 场景下，使用 GPU 比 CPU 快 3-5 倍。但要注意，不是所有操作都能 GPU 加速：只支持特定的算子（如 filter、project、join 等），如果遇到不支持的算子会 fallback 到 CPU。

**4. Spark Connect（3.4+）**

```scala
// - 解耦 Driver 和 Client
// - 支持远程提交代码到集群
// - 支持 Pandas/DuckDB 互操作

// 启动 Spark Connect Server：
// ./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0

// 客户端连接：
// spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
```

Spark Connect 是 Spark 3.4 引入的架构级变革。它把 Client 和 Driver 拆开——Client 不再需要部署 Spark 环境，只需要一个轻量级的客户端 SDK，通过 gRPC 协议与 Spark 集群通信。

**5. DSv2 数据源 v2 API**

```scala
// - 支持更多的数据源操作（分区、统计、谓词下推）
// - 相比 DSv1（DataSource v1）的主要改进：
//   1. 支持分区裁剪（Partition Pruning）
//   2. 支持列裁剪（Column Pruning）
//   3. 支持统计信息（Statistics）上报
//   4. 支持谓词下推（Predicate Pushdown）
//   5. 支持批量读取（Batch Reading）
```

**各版本新特性汇总：**

| Spark 版本 | 主要新特性 |
|------------|-----------|
| 3.0 | AQE、DPP、Pandas UDAF、ANSI SQL、新 UI |
| 3.1 | AQE 稳定性增强、K8s 增强 |
| 3.2 | AQE 默认开启、PySpark 增强 |
| 3.3 | DSv2 增强、Parquet 性能优化 |
| 3.4 | Spark Connect、DSv2 进一步完善 |
| 3.5 | Spark Connect 增强、新内置函数 |

## 动态资源分配

在 Spark 1.2 就已经引入的特性，但很多人在实际生产中仍然使用固定 Executor 数量。动态资源分配能让 Spark 根据作业负载自动调整 Executor 数量，避免资源浪费。

```scala
// 根据负载动态调整 Executor 数量
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "10")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "200")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

```scala
// 动态分配策略：
// 负载高 → 申请更多 Executor（每次翻倍，但不超过 maxExecutors）
// 负载低 → 释放空闲 Executor（默认 60s 无 Task 释放）

// 调度策略：
// 有 pending Task → 申请新 Executor
// 无 pending Task → 开始倒计时释放空闲 Executor
// 每个 Executor 空闲超过 spark.dynamicAllocation.executorIdleTimeout（默认60s）→ 释放
```

**动态资源分配的核心参数：**

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.dynamicAllocation.enabled` | false | 是否启用动态分配 |
| `spark.dynamicAllocation.minExecutors` | 0 | 最小 Executor 数 |
| `spark.dynamicAllocation.maxExecutors` | infinity | 最大 Executor 数 |
| `spark.dynamicAllocation.initialExecutors` | minExecutors | 初始 Executor 数 |
| `spark.dynamicAllocation.executorIdleTimeout` | 60s | 空闲 Executor 释放超时 |
| `spark.dynamicAllocation.schedulerBacklogTimeout` | 1s | 触发申请的等待时间 |
| `spark.dynamicAllocation.sustainedSchedulerBacklogTimeout` | schedulerBacklogTimeout | 持续触发申请的等待时间 |

**动态资源分配的优缺点：**

| 优点 | 缺点 |
|------|------|
| 提高集群资源利用率——作业不用独占资源 | Executor 频繁启停有开销（注册、反序列化） |
| 多个作业并发时公平调度 | 需要 External Shuffle Service 支持 |
| 大作业自动获取更多资源 | 对短作业效果不明显（启动时间占比高） |
| 避免高峰期手动调整 | 需要 YARN/K8s 资源管理器支持 |

**踩坑经验：**
- 动态资源分配在 YARN 模式下需要开启 External Shuffle Service，否则释放的 Executor 上的 shuffle 数据会丢失
- 不要和 `spark.executor.instances` 同时设置——后者是固定值，两个一起设会冲突
- 对于 ETL 作业（多个 Stage 连续执行，中间无空闲），动态分配的效果有限——资源一直用着，不会触发释放
- 建议给 `minExecutors` 设置一个合理的最小值，避免空跑时资源太少

```bash
# YARN 模式下 External Shuffle Service 配置：
# 在 yarn-site.xml 中：
# <property>
#   <name>yarn.nodemanager.aux-services</name>
#   <value>mapreduce_shuffle,spark_shuffle</value>
# </property>
# <property>
#   <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
#   <value>org.apache.spark.network.yarn.YarnShuffleService</value>
# </property>
```

> **面试点**：动态资源分配在什么场景下收益最大？答案是——集群中同时运行多个 Spark 作业，且各个作业的数据量差异较大时。每个作业按需获取资源，不会出现"大作业占着茅坑不拉屎"的情况。

## 面试高频考点

### Q: YARN cluster 和 client 模式的区别？

- **client**：Driver 运行在提交机器上（如开发机），可以看 stdout 日志，适合调试
- **cluster**：Driver 运行在 YARN 容器中（集群某节点），日志需从 YARN 查看

**深入对比：**

| 维度 | client 模式 | cluster 模式 |
|------|-------------|-------------|
| Driver 位置 | 提交机器 | YARN ApplicationMaster 内部 |
| 日志访问 | 直接控制台查看 | 需 `yarn logs -applicationId <appId>` |
| 故障恢复 | Driver 退出 -> 作业失败 | YARN ResourceManager 重启 ApplicationMaster |
| 资源消耗 | 占用提交机器资源 | 全部在集群内，资源统一管理 |
| 与 HDFS 交互 | 提交机需要能访问 HDFS | 不需要提交机访问 HDFS |
| 网络架构 | 提交机需要与 Executor 通信 | 所有通信在集群内部 |
| 生产推荐 | 不推荐 | **强烈推荐** |

生产用 cluster（Driver 故障由 YARN 重启），调试用 client。

### Q: AQE 的三个核心优化是什么？

1. **动态合并分区**：Shuffle 后自动合并小分区，减少 Task 数量，降低调度开销
2. **动态 Join 策略**：运行时判断小表是否可广播，自动转为 BroadcastHashJoin
3. **动态处理倾斜**：自动检测并拆分倾斜分区，消除长尾 Task

**进阶理解：**
- 三个优化按执行顺序排列：先合并分区 -> 调整 Join 策略 -> 处理倾斜
- 三者可以独立开启：`spark.sql.adaptive.coalescePartitions.enabled`、`spark.sql.adaptive.join.enabled`、`spark.sql.adaptive.skewJoin.enabled`
- Spark 3.2+ 中这三个优化默认全部开启

### Q: Spark 3.0 AQE 如何解决数据倾斜？

AQE 在 Shuffle 之后统计每个分区的数据量。如果某个分区数据量超过中位数的 5 倍且大于 256MB，AQE 会将该分区拆分为多个子分区，分别进行 Join 操作。

**具体流程：**

1. **检测阶段**：Shuffle 写完成后，AQE 读取每个分区的 shuffle 文件大小
2. **判断阶段**：计算所有分区大小的中位数，找出超过 `中位数 * skewedPartitionFactor(默认5)` 且大于 `skewedPartitionThresholdInBytes(默认256MB)` 的分区
3. **拆分阶段**：将倾斜分区拆分为多个子分区（每个子分区目标大小 = 非倾斜分区的 median 值）
4. **执行阶段**：每个子分区独立进行 Join，结果做 Union

```scala
// 如果同时存在多个倾斜分区，AQE 会逐个拆分
// 例如：A 分区 1GB，B 分区 800MB，其他分区平均 50MB
// → A 拆成 ~20 个子分区，B 拆成 ~16 个子分区
// → 每个子分区约 50MB，和其他分区大小相近
```

**AQE 无法解决的数据倾斜场景：**
- 聚合操作的 Map 端倾斜（如 groupBy 某个热点 key）——AQE 只优化 Join 倾斜，不优化聚合倾斜
- Shuffle 之前的 stage 内部计算倾斜
- 非 Shuffle 操作导致的倾斜（如 mapPartition 内处理不均匀）

### Q: 你用过哪些 Spark 部署方式？

典型回答：本地开发用 local 模式，测试环境用 YARN client 模式用于调试，生产环境用 YARN cluster 模式。容器化趋势下，新项目可以考虑 K8s 部署。

**加分回答思路：**

> 我在之前的项目中主要使用 YARN cluster 模式部署 Spark 作业。原因是我们已经有成熟的 Hadoop 集群，YARN 提供了良好的多租户隔离和资源管理能力。对于 Spark Streaming 作业，我们开启了动态资源分配，配合 External Shuffle Service，在低峰期自动释放空闲 Executor，高峰期自动申请。最近也在调研 Spark on K8s，计划将新业务迁移到 K8s 上，利用其更细粒度的资源隔离和更快的弹性伸缩能力。

**回答要点：**
- 说清楚你用了什么模式
- 说出为什么选这个模式（选型理由）
- 提到关键配置（如动态资源分配）
- 展示你对新技术的关注（如 K8s）

### Q: 动态资源分配和 AQE 有什么关系？

这是一个很好的进阶问题。动态资源分配和 AQE 是不同层面的优化：

- **动态资源分配**：**调度层面**的优化——根据作业负载动态调整 Executor 数量，解决"资源应该给多少"的问题
- **AQE**：**执行层面**的优化——根据数据分布动态调整执行计划，解决"数据应该怎么处理"的问题

两者可以协同工作：动态资源分配保证了资源充足，AQE 在资源充足的前提下高效利用这些资源。

## 小结

| 特性 | 说明 | 引入版本 |
|------|------|---------|
| AQE | 运行时动态优化查询 | 3.0 |
| 动态分区裁剪 | 自动推送分区过滤 | 3.0 |
| ANSI SQL | 严格 SQL 模式 | 3.0 |
| GPU 加速 | RAPIDS 插件 | 3.0 |
| Spark Connect | 远程 Driver-Client 解耦 | 3.4 |
| 动态资源 | 根据负载扩缩容 | 1.2+ |
| K8s 原生 | 容器化部署 | 2.3+ |

**一句话总结：**

部署模式选择要根据公司基础设施来决定，没有银弹；Spark 3.x 的核心变化是让 Spark 从"静态执行"走向"动态自适应"，AQE 是这个转变的标志性特性。如果你还在用 Spark 2.x，升级到 3.x 带来的性能提升几乎不需要改代码——开箱即用。
