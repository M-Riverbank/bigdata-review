# Spark 任务调度与内存管理

## Spark 运行时架构

> 很多初学者学 Spark 看了半天代码，却忽略了最根本的问题：**我写的 `rdd.map(...).reduceByKey(...)` 这段代码，到底是在哪台机器上跑的？谁在管它？**

搞清楚 Spark 运行时架构，是理解任务调度的第一步。我们先从整体视角看一张架构图。

```
┌──────────────────────────────────────────────────────────────┐
│                        Driver                                │
│  ┌────────────────┐  ┌────────────────┐  ┌────────────────┐  │
│  │  SparkContext   │  │  DAGScheduler  │  │  TaskScheduler  │  │
│  │  (应用的"大脑")   │  │  (画 DAG 图)    │  │  (分派 Task)    │  │
│  └────────────────┘  └───────┬────────┘  └───────┬────────┘  │
│                               │                    │           │
└───────────────────────────────┼────────────────────┼───────────┘
                                 │                    │
                      ┌─────────▼────────┐          │
                      │ Cluster Manager   │          │
                      │ (YARN / K8s /     │          │
                      │  Standalone)      │          │
                      └─────────┬────────┘          │
                                 │                    │
       ┌─────────────────────────┼────────────────────┼──────────────────┐
       │                         │                    │                  │
┌──────▼─────┐           ┌──────▼─────┐      ┌──────▼─────┐    ┌──────▼─────┐
│  Executor   │           │  Executor   │      │  Executor   │    │  Executor   │
│   (JVM)     │           │   (JVM)     │      │   (JVM)     │    │   (JVM)     │
│ ┌─────────┐ │           │ ┌─────────┐ │      │ ┌─────────┐ │    │ ┌─────────┐ │
│ │ Task    │ │           │ │ Task    │ │      │ │ Task    │ │    │ │ Task    │ │
│ │ Task    │ │           │ │ Task    │ │      │ │ Task    │ │    │ │ Task    │ │
│ │ Block   │ │           │ │ Block   │ │      │ │ Block   │ │    │ │ Block   │ │
│ │ Manager │ │           │ │ Manager │ │      │ │ Manager │ │    │ │ Manager │ │
│ └─────────┘ │           │ └─────────┘ │      │ └─────────┘ │    │ └─────────┘ │
└─────────────┘           └─────────────┘      └─────────────┘    └─────────────┘
```

整个架构可以总结为一句话：**Driver 负责"想清楚干什么"，Executor 负责"埋头把活干完"**。

### 面试官最喜欢问的问题：Driver 和 Executor 到底谁干了啥？

| 维度 | Driver | Executor |
|------|--------|----------|
| 数量 | 1 个（Application 一个） | 多个（可动态调整） |
| 核心职责 | 调度、解析、分配 | 执行、存储、汇报 |
| 数据存储 | 不存实际数据 | 通过 BlockManager 管理数据 |
| 容错处理 | 决定重试策略 | 执行失败时通知 Driver |
| 生命周期 | 从提交到结束 | 跟 Application 同生共死 |

> **面试点**：问你"Spark 作业在哪里跑的"——不是只在 Driver 也不是只在 Executor，是 Driver 调度、Executor 执行，二者配合完成。

### Driver 职责

Driver 是 Spark Application 的"总指挥官"。你的代码在 Driver 上被解析成执行计划，然后分发给各个 Executor 去执行。

具体来说，Driver 扛了下面这五件事：

1. **将用户代码翻译为 DAG**（由 DAGScheduler 完成）
   - 你的 Scala/Python 代码 → RDD 的血缘关系 → 有向无环图（DAG）

2. **依据 Shuffle 依赖划分 Stage**
   - 沿着 DAG 找"宽依赖"（ShuffleDependency），见一个切一刀

3. **将 Stage 以 TaskSet 提交给 TaskScheduler**
   - 每个 Stage 被封装成一个 TaskSet，提交到 TaskScheduler 的调度队列

4. **调度 Task 到 Executor 执行**
   - TaskScheduler 决定"哪个 Task 应该发给哪个 Executor"

5. **跟踪 Task 执行状态，处理失败重试**
   - 哪个 Task 挂了？需要重试吗？整个 Stage 要重新跑吗？——Driver 统一决策

> **面试点**：Driver 挂了怎么办？——Spark 1.x 不支持 Driver 高可用，挂了整个 Application 就没了。Spark 2.x+ 配合 Cluster Manager（YARN/K8s）可以实现 Driver HA。

### 🚨 踩坑经验：Driver 内存不够也是生产环境常见问题

```scala
// 错误示范：把大量数据 collect 到 Driver
val allData = rdd.collect()  // 所有数据拉到 Driver，内存直接爆炸

// 正确做法：用 take() 或输出到文件
val sampleData = rdd.take(100)  // 只取前 100 条
rdd.saveAsTextFile("hdfs://...") // 写到分布式存储
```

生产环境中遇到过同事把 10 亿条日志 `collect()` 到 Driver，30G 内存的 Driver 直接 OOM，整个 Application 挂了。**记住：Driver 只管调度，不管存数据。**

### Executor 职责

Executor 是真正干活的"工人"。每个 Application 有自己独立的 Executor 集合，不同 Application 的 Executor 互相隔离。

Executor 核心要做四件事：

1. **接收并执行 Task**
   - Driver 把 Task 序列化后通过网络发过来，Executor 反序列化后用线程池执行

2. **向 Driver 汇报 Task 结果**
   - 小结果直接回传，大结果（超过 `spark.driver.maxResultSize`）写入磁盘再回传路径

3. **提供内存 / 磁盘存储（BlockManager）**
   - BlockManager 是 Executor 内部的存储引擎，负责缓存 RDD 数据、Shuffle 中间结果等

4. **每个应用独享 Executor，相互隔离**
   - YARN 模式下，一个 NodeManager 可以启动多个 Executor，但每个 Executor 只服务一个 Application

> **面试点**：Executor 之间能直接通信吗？——Shuffle 的时候可以！Shuffle write 阶段 Map Output 需要被 Reducer 拉取，这时候 Reducer 的 Executor 会直接向 Mapper 的 Executor 发起网络请求（Netty）。但除此之外，Executor 之间不直接通信，统一由 Driver 协调。

### Executor 的 Task 运行机制

```scala
// Executor 内部本质是一个线程池
// 每个 Task 对应一个线程

// 查看 Task 并发数
val coresPerExecutor = spark.executor.cores  // 默认 1
// 假设 spark.executor.cores = 4
// 意味着这个 Executor 可以同时跑 4 个 Task

// 每个 Executor 的并行度 = spark.executor.cores
// 每个 core 同一时间只能跑一个 Task
```

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `spark.executor.cores` | YARN 上 1，Standalone 上所有可用 core | 每个 Executor 的 core 数 |
| `spark.executor.memory` | 1g | 每个 Executor 的 JVM 堆内存 |
| `spark.executor.instances` | 2 | 初始 Executor 数量 |
| `spark.dynamicAllocation.enabled` | false | 是否开启动态调整 Executor |

> **面试点**：Executor core 数设多少合适？——HDFS 读写密集任务建议 3-5 个 core，因为 HDFS 吞吐量的瓶颈不在 CPU；计算密集任务可以多设，但要考虑 GC 开销（core 多了 GC pause 也会增加）。

---

## DAGScheduler 详解

> 上节课我们讲了 Spark Application 的完整提交流程，从 `spark-submit` 到 Driver 启动。现在到了最核心的一环：**Driver 是怎么把我那段 `wordCount` 代码，一步步变成在集群上跑的 Task 的？**

答案就在 DAGScheduler。它是 Spark 调度体系中最聪明、最核心的组件。

### Job → Stage → Task

Spark 的调度体系可以概括为三层抽象：

```
┌─────────────────────────────────────────────────────┐
│                     Application                       │
│  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  ┌──────┐  │
│  │ Job1 │  │ Job2 │  │ Job3 │  │ Job4 │  │ ...  │  │
│  └──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘  └──────┘  │
│     │          │          │          │               │
│  ┌──▼──────────▼──────────▼──────────▼──────┐       │
│  │           Stage 划分                        │       │
│  └──┬──────────┬──────────┬──────────┬──────┘       │
│     │          │          │          │               │
│  ┌──▼──┐   ┌──▼──┐   ┌──▼──┐   ┌──▼──┐            │
│  │Task │   │Task │   │Task │   │Task │            │
│  │Set  │   │Set  │   │Set  │   │Set  │            │
│  └─────┘   └─────┘   └─────┘   └─────┘            │
└─────────────────────────────────────────────────────┘
```

对应关系非常清晰：

- **1 个 Application** = N 个 Job（每个 Action 算子触发一个 Job）
- **1 个 Job** = N 个 Stage（根据 Shuffle 依赖划分）
- **1 个 Stage** = 1 个 TaskSet（Task 集合）
- **1 个 TaskSet** = N 个 Task（对应 RDD 分区）

具体怎么划分？来看一个经典例子：

```
Action (collect/save/...) → DAGScheduler 触发

1. 从 Action RDD 向前回溯 → 遇到 ShuffleDependency 切一刀
2. 每个 ShuffleDependency 之前是一个 Stage
3. 最前面的是 ResultStage（Action 所在）
4. ShuffleMapStage 产生 Shuffle 数据

Job:      textFile → map → reduceByKey → collect
                         ↑ 切分点
Stage 0:  textFile → map  (ShuffleMapStage)
Stage 1:  reduceByKey → collect  (ResultStage)
```

> **面试点**：为什么 Stage 0 叫 ShuffleMapStage，Stage 1 叫 ResultStage？——ShuffleMapStage 的输出结果是给下游 Stage 用的（写 Shuffle 数据），ResultStage 的输出直接给用户（collect/save 的结果）。

### 🚨 踩坑经验：宽依赖过多导致 Stage 过多

```scala
// 非常常见的"踩坑"代码
val rdd1 = sc.textFile("hdfs://data/events")
val rdd2 = rdd1.groupByKey()    // 第 1 次 Shuffle → Stage 切分
val rdd3 = rdd2.mapValues(_.sum)
val rdd4 = rdd3.join(otherRDD)  // 第 2 次 Shuffle → Stage 切分
val result = rdd4.collect()

// 每个 Shuffle 产生一个新的 Stage
// 这个例子里有 3 个 Stage
// Stage 0: textFile
// Stage 1: groupByKey
// Stage 2: join → collect
```

每次 Shuffle 都意味着磁盘 I/O + 网络传输 + 序列化开销。**生产上要尽量减少 Shuffle 次数**，能用 `reduceByKey` 就别用 `groupByKey`。

| 算子 | Shuffle 行为 | 性能 |
|------|------------|------|
| `reduceByKey` | map 端预聚合，减少传输 | ⭐⭐⭐⭐⭐ |
| `groupByKey` | 全部数据传输，不预聚合 | ⭐⭐ |
| `join` | 两边都需要 Shuffle | ⭐⭐⭐ |
| `map`/`filter` | 窄依赖，无 Shuffle | ⭐⭐⭐⭐⭐ |

### Stage 内部

每个 Stage 内部的 Task 数量是怎么确定的？

```scala
// 一个 Stage 内部的 Task 数量 = 该 Stage 最后一个 RDD 的分区数

// Stage 0: textFile → map
//   textFile 有 HDFS 输入分片数 → N 个 Task
//   → 每个 Task 处理一个 HDFS Block（默认 128MB）
//
// 举例：HDFS 上有 100 个文件块，每个 128MB
//       → Stage 0 有 100 个 Task
//       → 每个 Task 处理 1 个 Block

// Stage 1: reduceByKey → collect
//   reduceByKey 的分区数 = spark.default.parallelism
//   → P 个 Task（通常 ≠ N）
//
// 举例：spark.default.parallelism = 200
//       → Stage 1 有 200 个 Task
//       → 但 Stage 0 才 100 个 Task
//       → 这是正常的，前后 Stage 分区数可以不一样
```

| 场景 | Task 数量计算公式 | 举例 |
|------|------------------|------|
| 读取 HDFS 文件 | HDFS 文件块数量 | 10GB 文件，128MB/块 → 80 个 Task |
| 读取小文件集合 | 文件数量（不合并的话） | 1000 个 1KB 小文件 → 1000 个 Task |
| Shuffle 后阶段 | `spark.default.parallelism` 或 `xxx.partitions` | 默认 200 |
| repartition 后 | 指定数量 | `rdd.repartition(500)` → 500 |

> **面试点**：Stage 内的 Task 之间是什么关系？——**完全并行且无依赖**。同一 Stage 的 Task 处理不同的分区，互不干扰，这是 Spark 能够并行计算的基础。

### DAGScheduler 的窄依赖与宽依赖判断

DAGScheduler 划分 Stage 的核心依据就是区分窄依赖和宽依赖：

```scala
// 窄依赖（NarrowDependency）
// 父 RDD 的每个分区最多被子 RDD 的 1 个分区使用
// 不需要 Shuffle
map, filter, union, mapPartitions, flatMap

// 宽依赖（ShuffleDependency）
// 父 RDD 的每个分区可能被子 RDD 的多个分区使用
// 需要 Shuffle
groupByKey, reduceByKey, join（非 co-partitioned）, distinct, repartition
```

| 特性 | 窄依赖 | 宽依赖 |
|------|--------|--------|
| Shuffle | 不需要 | 需要 |
| 数据本地性 | 好（pipelined 执行） | 差（需要网络传输） |
| 容错恢复 | 快（只需重新计算丢失分区） | 慢（需要重算整个父 Stage） |
| 同 Stage？ | 可以合并到同一 Stage | 必须切分新 Stage |
| 典型算子 | map, filter, union | reduceByKey, join, groupByKey |

### 失败处理

Spark 的容错机制分成三个层次：

```scala
// DAGScheduler 的容错策略：
// 1. 一个 Task 失败 → 重试（默认 4 次，spark.task.maxFailures）
// 2. 一个 Stage 失败 → 重新提交整个 Stage
// 3. ShuffleMapStage 输出丢失 → 重新计算父 Stage

// 重点理解第三点：
// 为什么 ShuffleMapStage 输出丢失需要重算父 Stage？
// 因为 Shuffle 输出的中间数据是存 Executor 本地磁盘的
// Executor 挂了 → 数据丢了 → 没人有这个输出 → 只能重算
```

容错机制对比：

| 失败类型 | 恢复方式 | 恢复代价 |
|----------|---------|----------|
| Task 失败（节点抖动） | TaskScheduler 重试 | 低（只重跑 1 个 Task） |
| Executor 失败 | DAGScheduler 重新提交所属 Stage | 中（重跑该 Stage 所有 Task） |
| Shuffle 数据丢失 | DAGScheduler 重算父 Stage | 高（重跑上游所有 Stage） |
| Driver 失败 | Cluster Manager 重新拉起 Driver | 最高（整个 Application 重启） |

```scala
// 容错相关配置
spark.task.maxFailures = 4           // Task 最大重试次数（默认 4）
spark.speculation = false            // 是否开启推测执行
spark.speculation.interval = 100ms   // 推测执行检测间隔
spark.speculation.multiplier = 1.5   // 比中位数慢多少倍时启动推测
```

> **面试点**：推测执行（Speculative Execution）有什么用？——当集群中存在"慢节点"（straggler）时，Driver 会在另一个节点启动相同 Task 的副本，谁先跑完就用谁的结果。**但要注意**：写操作（如 `saveAsTextFile`）开启推测执行可能导致数据重复写入，需要 `OutputCommitter` 来处理幂等性。

### 🚨 踩坑经验：推测执行踩过的坑

```
现象：写入 HDFS 的文件量翻倍或者文件内容重复
原因：推测执行让两个 Task 写同一份数据
解决：关闭推测执行（适合写密集型作业）
    spark.speculation = false
    或者使用支持推测的 OutputCommitter
```

---

## TaskScheduler 详解

> DAGScheduler 把 Stage 划分好，封装成 TaskSet 之后就放手了。**谁来把 Task 实际发到 Executor 上去跑？**——这就轮到 TaskScheduler 登场了。

如果把 DAGScheduler 比作"总设计师"，TaskScheduler 就是"现场施工队长"：设计师画好图纸，施工队长找人干活。

```scala
// TaskScheduler 负责：
// 1. 将 TaskSet 中的 Task 分发到 Executor
// 2. 处理 Task 失败时通知 DAGScheduler
// 3. 返回 Task 执行结果给 DAGScheduler

// 工作流程：
// 1. DAGScheduler 提交 TaskSet → TaskScheduler
// 2. TaskScheduler 为每个 Task 创建 TaskSetManager
// 3. TaskSetManager 负责跟踪该 TaskSet 中每个 Task 的执行状态
// 4. 当有 Executor 心跳汇报空闲资源 → TaskScheduler 分配 Task
// 5. Task 执行完成 → 结果返回 TaskScheduler → 通知 DAGScheduler
```

### 两种调度策略

```scala
// 调度策略（两种）：
// FIFO（默认）：先入先出，适用于单用户
spark.scheduler.mode = FIFO

// FAIR：多 Job 公平共享资源
spark.scheduler.mode = FAIR
```

| 特性 | FIFO | FAIR |
|------|------|------|
| 调度思想 | Job 按提交顺序排队执行 | 多个 Job 轮流转，分时间片 |
| 适用场景 | 单个用户/单个 Job | 多用户/多 Job 并发 |
| 饥饿问题 | 大 Job 可能阻塞小 Job | 无饥饿 |
| 资源利用 | 高（集中资源干一件事） | 中（资源分散） |
| 配置复杂度 | 0（默认就开箱即用） | 需要配置 pools 和权重 |

> **面试点**：如果一个用户提交了大的 ETL 任务（跑 30 分钟），另一个用户提交了一个小查询（只需 5 秒），用 FIFO 会怎样？——大 Job 先占用所有资源，小 Job 需要等大 Job 跑完才能开始。所以在多用户场景下强烈建议使用 FAIR 调度模式。

### FAIR 调度模式详解

```xml
<?xml version="1.0"?>
<!-- fairscheduler.xml 配置示例 -->
<allocations>
  <pool name="production">
    <schedulingMode>FAIR</schedulingMode>
    <weight>3</weight>
    <minShare>10</minShare>
  </pool>
  <pool name="default">
    <schedulingMode>FIFO</schedulingMode>
    <weight>1</weight>
    <minShare>2</minShare>
  </pool>
</allocations>

<!-- weight=3 → production 池获得的资源是 default 池的 3 倍 -->
```

### 数据本地性（Data Locality）

> 这是 Spark 面试中**最高频的考点之一**。数据本地性的核心思想：**把计算移动到数据所在的位置，而不是把数据移动到计算所在的位置。**

为什么？因为网络传输数据比本地读取数据慢得多。HDFS 上 1GB 数据通过网络传输可能要十几秒，本地磁盘读取只要几秒。

```
优先级（从高到低）：
1. PROCESS_LOCAL — 数据在同一 JVM 中（最快）
   场景：RDD 被缓存后、同一个 Executor 内的后续操作

2. NODE_LOCAL    — 数据在同一节点
   场景：从 HDFS 读取时，数据块在同一台机器上

3. NO_PREF       — 无位置偏好
   场景：从数据库读取、纯计算任务

4. RACK_LOCAL    — 数据在同一机架
   场景：数据在同一个机架的不同节点上

5. ANY           — 任意位置（需跨网络）
   最差情况：数据在别的机架上，需要跨网络传输

spark.locality.wait = 3s（等待更高本地性的最长时间）
```

### 数据本地性降级机制

```
  PROCESS_LOCAL → 等 3s → 还没等到空闲 slot → 降级到 NODE_LOCAL
  NODE_LOCAL    → 等 3s → 还没等到              → 降级到 RACK_LOCAL
  RACK_LOCAL    → 等 3s → 还没等到              → 降级到 ANY

  "数据显示在面前都不取 → 等 3s → 降级到下一级 → 再次等待 → 最终降级到 ANY"
```

> **面试点**：如果一个 Executor 正在处理某个分区时 GC 停顿了 5 秒，本地性等待会怎么样？——`spark.locality.wait = 3s`，超时就降级了。这个问题在生产中经常出现，可以通过调大 `spark.locality.wait`（比如调到 5s）或者调优 GC 来解决。

### 🚨 踩坑经验：数据本地性导致的性能陷阱

```scala
// 场景：Spark Streaming 消费 Kafka 数据
// 问题：Task 调度时数据本地性一直是 ANY
// 原因：Kafka 数据在 Broker 上，Spark Executor 在别的节点
//      → 永远达不到 PROCESS_LOCAL 或 NODE_LOCAL
// 解决：使用 Direct Kafka API + 偏好分区调度

// 配置调优建议：
spark.locality.wait = 3s   // 默认值，大多数场景够用
// 如果集群网络很好，可以调小
spark.locality.wait = 1s
// 如果集群跨机房、网络延迟高，可以调大
spark.locality.wait = 5s
```

| 场景 | 本地性级别 | 优化建议 |
|------|-----------|---------|
| 从 HDFS 读取 | NODE_LOCAL | 调整 HDFS 副本策略，确保数据在计算节点有副本 |
| 缓存 RDD 重用 | PROCESS_LOCAL | 确保缓存策略是 MEMORY_ONLY 或 MEMORY_AND_DISK |
| Kafka Direct Stream | PREFERRED_LOCAL | 使用 `KafkaUtils.createDirectStream` |
| 纯计算（无数据读取） | NO_PREF | 不需要优化，跨节点传输计算逻辑，不传数据 |

---

## Spark 内存管理

> 这个问题很经典——"**为什么我的 Spark 作业明明有 200G 内存，还是 OOM 了？**"
>
> 答案往往不是你内存不够，而是你不了解 Spark 的内存管理机制。

Spark 的内存管理经历了两个时代：Spark 1.6 之前是静态内存管理（Static Memory Manager），1.6+ 之后是统一内存管理（Unified Memory Manager）。我们先看旧的，再对比新的，这样你就能理解为什么 Spark 社区要改。

### 1.6 前：静态内存管理

在 Spark 1.6 之前，内存区域是**硬隔离**的，区域之间不能互相借用：

```
堆内存总量
├── Storage（缓存 RDD）: 60% × (1 - 0.2) = 48%
│   └── 不可借用
├── Execution（Shuffle）: 20% × (1 - 0.2) = 16%
│   └── 不可借用
└── Other: 20%
```

这个方案最大的问题是：**Storage 区域用不完的时候，Execution 不能借用；Execution 用不完的时候，Storage 也不能借用。**

举个例子：
- 你的作业只需要 `cache()` 很少的数据，Storage 只用了 10%，剩下 38% 浪费着
- 但 Shuffle 阶段需要大量内存做排序和聚合，Execution 只有 16%，根本不够用
- 结果：Shuffle 阶段频繁 spill 到磁盘，性能急剧下降

> **面试点**：静态内存管理的问题是什么？——内存利用率低。各个区域之间有"隔离墙"，无法动态调整，导致一边内存闲置、一边内存不足。

### 1.6+ 统一内存管理（Unified Memory Manager）

这就是 Spark 1.6 带来的重大改进，也是当前所有 Spark 版本（2.x、3.x）的默认方案：

```
堆内存总量 = spark.executor.memory（例如 4G）

Reserved Memory = 300MB（系统保留，不可配置）

├── User Memory = (总 - Reserved) × (1 - spark.memory.fraction)
│   └── 用户数据 + 元数据
│
└── Spark Memory = (总 - Reserved) × spark.memory.fraction（默认 0.6）
    ├── Storage Memory = Spark Memory × spark.memory.storageFraction（默认 0.5）
    │   └── 缓存 RDD/DataFrame
    └── Execution Memory = 剩余部分
        └── Shuffle / Join / Sort / Aggregation

    动态借用机制：
    - Execution 可以向 Storage 借用（收回缓存的数据）
    - Storage 不能向 Execution 借用（保证计算不 OOM）
```

看一个具体计算例子：

```scala
// Executor 配置 4G 内存
// spark.memory.fraction = 0.6（默认）
// spark.memory.storageFraction = 0.5（默认）

// Reserved = 300MB
// Spark Memory = (4096 - 300) × 0.6 = 2277.6 MB
// User Memory = (4096 - 300) × 0.4 = 1518.4 MB
// Storage = 2277.6 × 0.5 = 1138.8 MB
// Execution = 2277.6 - 1138.8 = 1138.8 MB
```

```
┌─────────────────────────────────────────────────────────────┐
│                    JVM Heap (4G)                             │
│  ┌───────────────────────────────────────────────────────┐  │
│  │     Reserved Memory (300MB) — 存 Spark 内部对象       │  │
│  ├───────────────┬───────────────────────────────────────┤  │
│  │               │          Spark Memory (2.28G)          │  │
│  │  User Memory  │  ┌──────────────────┬───────────────┐  │  │
│  │   (1.52G)     │  │  Storage (1.14G) │ Execution     │  │  │
│  │               │  │  ←─ 可被借用 ──   │  (1.14G)     │  │  │
│  │  用户代码、    │  │   cache/persist   │  Shuffle     │  │  │
│  │  UDF 数据、   │  │   Broadcast       │  Join/Sort   │  │  │
│  │  元数据       │  │                   │  Aggregate   │  │  │
│  │               │  └──────────────────┴───────────────┘  │  │
│  └───────────────┴────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 动态借用机制详解

统一内存管理最核心的改进就是这个**动态借用机制**：

```scala
// 场景 1：Execution 不够用了
// Storage 还有空闲 → Execution 可以直接借用 Storage 的空间
// 如果 Storage 用到了被占用的空间 → Storage 的块会被驱逐（evict）
// 被驱逐的块如果有磁盘备份（persist 设了 disk 级别）→ 写到磁盘
// 如果没有磁盘备份（MEMORY_ONLY）→ 直接丢掉，需要时重新计算

// 场景 2：Storage 需要更多空间
// Execution 占用了 Storage 的空间 → Storage 不能强制收回
// Execution 用完自动释放 → Storage 才能重新使用
// 这就是"单向借用"：Execution 可以拿 Storage 的，反过来不行
```

> 🚨 **面试点**：为什么设计成"Execution 可以向 Storage 借用，反过来不行"？——核心原因是 Execution 内存的使用是**不可替代**的。如果 Shuffle 过程中 Execution 内存不足，要么 spill 到磁盘（严重降低性能），要么直接 OOM。而 Storage 缓存的数据即使被 evict，最多只是需要重新计算，不会导致作业失败。**计算优先于缓存。**

### 各内存区域详细说明

| 区域 | 用途 | 如果设置过小 | 如果设置过大 |
|------|------|-------------|-------------|
| Reserved (300MB) | 存储 Spark 内部对象 | OOM/内部错误 | 不能调整，固定值 |
| User Memory | UDF 数据、用户对象 | UDF OOM | 压缩 Spark Memory 空间 |
| Storage | 缓存 RDD/DataFrame | 缓存命中率低 | 浪费，Execution 不够 |
| Execution | Shuffle/Join/Sort | 频繁 spill 到磁盘 | 较少缓存空间 |

### 堆外内存

> 堆外内存（Off-Heap Memory）是 Spark 2.x 引入的高级特性，主要用于提升性能和减少 GC 开销。

```scala
spark.memory.offHeap.enabled = true  // 开启堆外内存
spark.memory.offHeap.size = 2g       // 堆外大小

// 堆外内存优势：
// 1. 不受 JVM GC 影响 → 减少 GC pause
// 2. 避免大对象在 GC 时频繁 promotion
// 3. 更适合 Tungsten 的二进制格式（直接操作内存地址）
// 4. 堆外内存 vs 堆内内存比较

// 堆外内存劣势：
// 1. 不归 JVM 管理，可能内存泄漏（需要手动管理生命周期）
// 2. 需要额外的序列化/反序列化（Java 对象 → 二进制字节）
// 3. 调试困难（无法通过 jmap/heap dump 查看）
```

| 特性 | 堆内内存 (On-Heap) | 堆外内存 (Off-Heap) |
|------|-------------------|-------------------|
| 管理方式 | JVM GC 自动管理 | 应用自行管理（sun.misc.Unsafe） |
| GC 影响 | 大对象导致 Full GC | 无 GC 影响 |
| 序列化 | 不需要 | 需要（对象→字节数组） |
| 内存上限 | `spark.executor.memory` 控制 | 独立配置 |
| 数据格式 | Java 对象 | Tungsten 二进制格式 |
| 适用场景 | 通用场景 | 大 Shuffle、大量缓存 |

### 🚨 踩坑经验：内存参数调优实战

```scala
// 场景：一个 200 核、500GB 内存的集群，处理 10TB 数据
// 作业每隔 30 分钟 OOM 一次

// 第一步：分析
// Executor 配置：spark.executor.memory = 20g, spark.executor.cores = 8
// 每个 Executor 的 Spark Memory = (20g - 300m) × 0.6 ≈ 11.8g
// Storage = 5.9g, Execution = 5.9g

// 问题分析：Execution 只有 5.9g，Shuffle 数据量太大，频繁 spill 甚至 OOM

// 第二步：调优
spark.executor.memory = 32g           // 增加内存
spark.memory.fraction = 0.75          // 提高 Spark Memory 比例（从 0.6 → 0.75）
spark.memory.storageFraction = 0.3    // 降低 Storage 比例（从 0.5 → 0.3）
// 调整后：Spark Memory = (32g - 300m) × 0.75 ≈ 23.8g
//         Execution = 23.8 × 0.7 = 16.66g（原来 3 倍！）

// 第三步：辅助优化
spark.shuffle.file.buffer = 64k       // Shuffle 写缓冲区
spark.reducer.maxSizeInFlight = 96m   // Shuffle 读缓冲区
spark.shuffle.spill.compress = true   // spill 数据压缩
```

### 内存配置参数速查表

| 参数 | 默认值 | 建议 | 说明 |
|------|--------|------|------|
| `spark.executor.memory` | 1g | 4-32g | 每个 Executor 的内存 |
| `spark.memory.fraction` | 0.6 | 0.6-0.8 | Spark Memory 占总堆的比例 |
| `spark.memory.storageFraction` | 0.5 | 0.3-0.6 | Storage 占 Spark Memory 比例 |
| `spark.memory.offHeap.enabled` | false | 大内存+大 Shuffle 时开启 | 堆外内存开关 |
| `spark.memory.offHeap.size` | 0 | 与堆内比例 1:1 | 堆外内存大小 |
| `spark.shuffle.memoryFraction` | 0.2 | 已废弃（1.6+ 统一管理） | 旧版参数 |

---

## 小结

> 回顾整篇文章，我们从"Spark 作业的代码到底在哪跑"这个问题出发，一路深入到了 Spark 最核心的调度机制和内存管理。

| 组件 | 关键职责 |
|------|---------|
| DAGScheduler | Job → Stage → TaskSet（按 Shuffle 宽依赖切分 Stage） |
| TaskScheduler | Task → Executor（考虑数据本地性和调度策略） |
| 数据本地性 | PROCESS_LOCAL > NODE_LOCAL > RACK_LOCAL > ANY（计算移动优于数据移动） |
| 统一内存 | Storage/Execution 动态借用，Execution 优先（计算优先于缓存） |
| Worker/Task | Executor 管多核，每个核一个 Task Slot |

### 面试价值点速记

> **面试点**（总结）：
> 1. **Stage 划分依据**：宽依赖（ShuffleDependency），见一个切一刀
> 2. **Task 数量**：Stage 最后一个 RDD 的分区数
> 3. **数据本地性**：PROCESS_LOCAL > NODE_LOCAL > NO_PREF > RACK_LOCAL > ANY
> 4. **统一内存**：Execution 可以借 Storage，反之不行——因为计算优先
> 5. **堆外内存**：减少 GC，适合大 Shuffle，但需要序列化
> 6. **调度模式**：FIFO（默认，单用户）、FAIR（多用户）

### 3 个必须记住的"不一定"

1. **Task 多不一定快**——Task 数量应该略大于总 core 数的 2-3 倍，太少浪费资源，太多增加调度开销
2. **内存大不一定不 OOM**——不了解内存区域划分和借用机制，给 100G 也可能 OOM
3. **重试多不一定好**——`spark.task.maxFailures` 设得太大，存在"硬错误"（bug/数据问题）时会白白浪费大量时间
