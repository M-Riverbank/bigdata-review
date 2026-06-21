# Spark Core — DAG 调度与 Stage 划分

## DAG 调度全流程

### 为什么需要 DAG 调度器？

如果你写过 Spark 程序，一定见过这样的代码：

```scala
val rdd = sc.textFile("hdfs://data/click_logs")
  .flatMap(_.split(" "))
  .map(word => (word, 1))
  .reduceByKey(_ + _)
  .collect()
```

看起来只是链式调用了几个算子，但 Spark 内部需要回答这些问题：

1. **哪些操作可以合并执行？** —— `flatMap` 和 `map` 都是逐元素操作，能不能在一个 Task 里做完？
2. **哪些操作需要网络传输？** —— `reduceByKey` 需要对相同 key 做聚合，不同节点上的数据必须 shuffle 到一起。
3. **怎样安排执行顺序？** —— 必须等 `reduceByKey` 的数据准备好，`collect` 才能执行。
4. **某个节点宕机了，要重算哪些部分？** —— 是重新跑整个 Job，还是只重跑丢失的分区？

这些问题全部由 **DAGScheduler** 负责。它是 Spark 调度体系的第一层大脑。

### 调度全流程概览

Spark 将用户代码转化为实际集群执行任务的核心流程：

```
用户代码 → DAGScheduler → TaskScheduler → Executor 执行
```

> 与 Hadoop MapReduce 的一个核心区别：MR 每个 Job 都固定 Map → Reduce 两个阶段，复杂任务必须手动串联多个 MR。**Spark 的 DAGScheduler 自动分析依赖关系，将 RDD 操作链切割为最优的 Stage DAG，一个 Job 可以包含任意多个 Stage。**

### 四步详解

```
1. 用户代码构建 RDD 依赖链（DAG）
   val rdd = sc.textFile("...")
     .flatMap(_.split(" "))
     .map((_, 1))
     .reduceByKey(_ + _)
     .collect()
       ↑ Action 触发

2. DAGScheduler 接收 Job
   → 从 collect() 向前回溯 RDD 依赖链
   → 遇到 ShuffleDependency 切割 Stage
   → 构建 Stage DAG

3. Stage 变为 TaskSet 提交给 TaskScheduler
   → 每个 Stage 包含多个 Task（一个分区一个 Task）
   → TaskScheduler 序列化 Task → 分发到 Executor

4. Executor 反序列化 Task → 执行计算
```

> **面试点**：DAG 调度是"延迟触发"的典型模式。RDD 上的 `map`、`filter`、`flatMap` 等 Transformation 操作只是构建依赖图，并不真正执行。只有遇到 `collect`、`count`、`saveAsTextFile` 等 Action 操作，DAGScheduler 才会收到一个 Job，开始 Stage 划分和调度。

### RDD Lineage（血统）的概念

DAG 结构本质上就是 RDD 的血统（Lineage）：

```
textFile("/data/log")
  ├── flatMap(_.split(" "))        ← 窄依赖
  │     └── map(word => (word, 1)) ← 窄依赖
  │           └── reduceByKey(_ + _) ← 宽依赖
  │                 └── collect()    ← Action
```

**血统的作用**：每个 RDD 都记录了它是如何从父 RDD 变换而来的。当某个分区数据丢失时，Spark 可以根据血统重新计算，无需检查点或持久化。

**血统 vs 传统容错对比**：

| 容错方式 | 原理 | 优点 | 缺点 |
|---------|------|------|------|
| Spark 血统 | 记录变换操作链 | 只重算丢失分区，避免数据复制 | 长链重算耗时大 |
| Hadoop 容错 | 中间结果落盘 HDFS | 重启快 | 大量磁盘 I/O |
| 传统检查点 | 定期持久化状态 | 恢复快 | 持续写开销 |

| 组件 | 作用 |
|------|------|
| DAGScheduler | 将 RDD 依赖链转为 Stage DAG |
| TaskScheduler | 将 TaskSet 分发到 Executor |
| 窄依赖 | 同 Stage pipeline 执行 |
| 宽依赖 | Stage 边界，需要 Shuffle |
| 数据本地性 | 尽量让 Task 和数据在同一位置 |
| 失败重试 | Task 重试 4 次，Stage 丢失重算 |

## DAGScheduler 工作细节

### Job 创建

#### Action → Job 的触发过程

Spark 中 **一个 Action 对应一个 Job**，而不是一个 RDD 或一个算子：

```scala
// 每个 Action 创建一个 Job
rdd.collect()      // → Job 0
rdd.count()        // → Job 1
rdd.saveAsTextFile(...)  // → Job 2
```

**常见误区**：初学者常以为一个 Spark 应用只有一个 Job。实际上，**代码中写了几个 Action，DAGScheduler 就创建几个 Job**。在 Spark UI 的 Jobs 标签页里，每个 Action 对应一个 Job ID。

**同一个 Action 调用多次也会生成多个 Job：**

```scala
val rdd = sc.textFile("data.log").filter(_.contains("ERROR"))
rdd.count()  // Job 0
rdd.count()  // Job 1 — 虽然 RDD 相同，但每次 Action 触发独立 Job
```

> **面试点**：如果同一个 RDD 被多次 Action 调用，每调用一次都要从头开始计算。这是为什么实际开发中需要 `rdd.cache()` 或 `rdd.persist()` 来避免重复计算。Spark UI 中如果看到同一个 Stage 被多个 Job 重复执行，通常就是没用缓存。

#### Job 提交的内部逻辑

DAGScheduler 接收到 Action 触发后，内部会执行：

```
1. 创建一个 Job 对象，分配 Job ID
2. 调用 runJob() 方法
3. 从最后一个 RDD（调用 Action 的那个 RDD）开始回溯依赖链
4. 根据宽依赖切分 Stage
5. 提交 Stage 到 TaskScheduler
```

### Stage 划分规则

#### 核心原则

```
核心规则：从后往前回溯，遇到宽依赖切分 Stage

Job:   A → B(map) → C(filter) → D(reduceByKey) → E(map) → F(collect)

从 F 往前：
F ← E: 窄依赖（map）
E ← D: 窄依赖（map）
D ← C: 宽依赖（reduceByKey）→ 切！D 开始新 Stage
C ← B: 窄依赖（filter）
B ← A: 窄依赖（map）

结果：
Stage 0 (ShuffleMapStage): A → B → C
Stage 1 (ShuffleMapStage): D
Stage 2 (ResultStage):    E → F
```

#### 为什么从后往前？

> **设计解读**：从后往前回溯的好处是，**只要确定最后一个 Stage 是 ResultStage（执行 Action），前面的 Stage 就是它的依赖**。这就像搭积木——先确定最终要什么（ResultStage），再向前规划需要哪些中间结果（ShuffleMapStage）。

举个更复杂的例子：

```scala
val rdd1 = sc.textFile("users.txt")
  .map(parseUser)                          // 窄
  .filter(_.age > 18)                      // 窄

val rdd2 = sc.textFile("orders.txt")
  .map(parseOrder)                         // 窄

val joined = rdd1.join(rdd2)               // 宽依赖！需要 shuffle
val result = joined.map(formatOutput)      // 窄
result.saveAsTextFile("/output")           // Action
```

划分结果：

```
Stage 0 (ShuffleMapStage): textFile → map → filter  (rdd1)
Stage 1 (ShuffleMapStage): textFile → map           (rdd2)
Stage 2 (ShuffleMapStage): join                     (shuffle 写入)
Stage 3 (ResultStage):    map → saveAsTextFile
```

**注意**：这里 rdd1 和 rdd2 的 Stage 0 和 Stage 1 是 **并行执行** 的！它们互不依赖，TaskScheduler 会同时调度。

> **面试点**：Stage 划分的优化思路——窄依赖越多，一个 Stage 内部用 pipeline 执行，效率越高。宽依赖越少，shuffle 次数越少，性能越好。所以可以用 **map-side combine**（如 `reduceByKey` 而非 `groupByKey`）来减少 shuffle 数据量。

#### Pipeline 执行原理

同一个 Stage 内的多个窄依赖操作在同一个 Task 中流水线执行：

```
Stage 0 的一个 Task：
  textFile → flatMap → map → filter
   ↑             ↑       ↑      ↑
  这些步骤在一个 Task 内串行完成，不分阶段
```

这就意味着：**Task 从 HDFS 读到一行数据后，立刻执行 flatMap → map → filter，处理完才算一个 record 完成**，而不是等全部数据 flatMap 完再做 map。

### ShuffleMapStage vs ResultStage

| 类型 | 说明 | 如何产生 | 输出 | 失败后果 |
|------|------|---------|------|---------|
| **ShuffleMapStage** | 产生中间数据供下游 Stage 读取 | 不是 Job 的最后一个 Stage | 写入磁盘（Shuffle 文件） | 下游需要重新获取 |
| **ResultStage** | 执行 Action 并返回结果 | Job 的最后一个 Stage | 返回 Driver 或写入外部存储 | Job 整体失败 |

```scala
// ShuffleMapStage 代码示例
val rdd = sc.textFile("hdfs://data/input")
  .flatMap(_.split(" "))       // 在 Stage 0 中 pipeline 执行
  .map(word => (word, 1))      // 在 Stage 0 中 pipeline 执行
  .reduceByKey(_ + _)          // 宽依赖，开启 Stage 1
  
// ResultStage 代码示例  
rdd.collect()                  // Action，Stage 2
```

> **踩坑经验**：在 Spark UI 中，**如果一个 Stage 的 Shuffle Read 量远大于 Shuffle Write 量**，说明数据在 shuffle 过程中被过度放大，常见原因是 key 分布不均匀（数据倾斜）。比如用 `groupByKey` 时，某个热门 key 的数据占到了所有数据的 70% 以上，对应 Executor 的 Task 就会特别慢。

#### 如何查看 Stage 信息

在 Spark UI（默认 `http://driver:4040`）的 **Stages** 标签页：

```
Stage 0 (ShuffleMapStage)
 ├── 输入: 128MB HDFS 数据
 ├── 输出: 256KB Shuffle 写入
 ├── Tasks: 8/8 (成功)
 └── 耗时: 12s

Stage 1 (ResultStage)
 ├── 输入: 256KB Shuffle 读取
 ├── Tasks: 4/4 (成功)
 └── 耗时: 3s
```

> **面试点**：看到大量 Shuffle 写入远大于输入数据量，说明 shuffle 过程中数据膨胀，可能是 join 或 groupBy 的笛卡尔积效应。看到 Shuffle Read 远大于 Shuffle Write，说明上游 Stage 的数据被下游拆散重分布了，join 场景下经常出现。

## RDD 依赖类型

### 为什么区分依赖类型？

DAGScheduler 依赖**依赖类型**来决定两件事：

1. **能否合并到同一个 Stage**——窄依赖可以 pipeline，宽依赖必须切 Stage
2. **分区丢失后如何恢复**——窄依赖只需重算父 RDD 的对应分区，宽依赖则需要重算所有父分区

### 窄依赖（NarrowDependency）

**定义**：每个父 RDD 分区最多被一个子 RDD 分区使用。

```scala
// 1:1 依赖 — map, flatMap, filter
// N:1 依赖 — coalesce（合并多个父分区到一个子分区）

// 窄依赖的优势：
// - 可以在同一个 Task 内流水线执行（pipeline）
// - 分区丢失只需重新计算对应父分区
// - 不需要网络传输
```

**三种子类型**：

| 类型 | 说明 | 算子举例 | 分区关系 |
|------|------|---------|---------|
| OneToOneDependency | 父分区与子分区一一对应 | `map`、`filter`、`flatMap` | 子分区 i ← 父分区 i |
| RangeDependency | 父分区范围对应子分区 | `union` | 子分区 0..N-1 ← 父 A，N.. ← 父 B |
| PruneDependency | 部分父分区参与 | `cogroup` 的部分场景 | 子集对应 |

**实际代码示例**：

```scala
// 窄依赖链 — 全部在同一个 Stage
val rdd = sc.parallelize(1 to 10000, 10)
  .filter(_ % 2 == 0)        // OneToOneDependency
  .map(x => (x % 10, x))      // OneToOneDependency
  .sortByKey()                // 依旧是窄依赖

// 查看依赖
rdd.dependencies.foreach(println)
// 输出: org.apache.spark.OneToOneDependency@xxxxxx
```

> **踩坑经验**：`coalesce(1, shuffle = false)` 使用的是窄依赖，但如果上游分区数很大，大量数据会聚集到同一个分区，可能导致 **OOM**。需要合并分区时，尽量评估数据量是否能在单个 Executor 内存中装下。

### 宽依赖（ShuffleDependency）

**定义**：父 RDD 的一个分区被子 RDD 的多个分区使用。

```scala
// 1:N 依赖 — reduceByKey, groupByKey, repartition, join
// 每个父分区需要被分发到不同的子分区

// 宽依赖的代价：
// - 需要网络 Shuffle
// - 父 Stage 必须全部完成后子 Stage 才能开始
// - 分区丢失需要重算所有父 Stage
```

**哪些算子会产生宽依赖？**

| 算子 | 产生原因 | 数据影响 |
|------|---------|---------|
| `reduceByKey` | 按 key 聚合，key 分散在所有分区 | shuffle 写量 = 原始数据量 / 分区数 |
| `groupByKey` | 按 key 分组 | shuffle 写量 = 原始数据量（不做 map-side 合并） |
| `repartition` | 重新分区 | 全部数据 shuffle |
| `coalesce(shuffle=true)` | 带 shuffle 的合并 | 全部数据 shuffle |
| `join` | 按 key 连接两个 RDD | shuffle 写量 = 两边数据量之和 |
| `distinct` | 去重需要按 key 聚合 | shuffle 写量 = 原始数据量 |

**宽依赖与数据倾斜**：

```scala
// 倾斜场景
val skewed = rdd.groupByKey()  // 某个 key 有海量数据
// → 该 key 所在分区的 Task 处理时间远超其他 Task
// → 整个 Stage 等待这个慢 Task

// 优化方案
val optimized = rdd
  .map(x => (x._1, x._2))
  .reduceByKey(_ ++ _)  // 先用 map-side combine 减少 shuffle 量
```

> **面试点**：宽依赖是 Spark 性能优化的**关键瓶颈**。面试常问："如何减少 Spark Shuffle？" 答案方向：
> 1. 使用 `reduceByKey` 代替 `groupByKey`（map-side combine）
> 2. 使用 broadcast join 代替 shuffle join（大表 join 小表）
> 3. 合理设置 `spark.sql.shuffle.partitions`
> 4. 使用两阶段聚合解决数据倾斜（加随机前缀 → 局部聚合 → 去除前缀 → 全局聚合）

### 如何查看依赖链

#### 通过代码查看

```scala
val rdd = sc.textFile("hdfs://data.log")
  .filter(_.contains("ERROR"))
  .map(line => (line.split(",")(0), 1))
  .reduceByKey(_ + _)

// 查看 RDD 依赖
rdd.dependencies.foreach(println)
// ShuffleDependency(...)  ← reduceByKey 是宽依赖

// 查看父 RDD
rdd.dependencies.flatMap(_.rdd.dependencies).foreach(println)
// NarrowDependency(...)   ← filter → map 是窄依赖
```

#### 通过 Spark UI 查看

Spark UI 的 **DAG Visualization** 是最直观的方式：

```
访问 http://driver:4040/jobs/job/?id=0

看到的 DAG 图（绿色矩形 = Stage，内部是算子）：
┌──────────────────┐
│ Stage 0          │
│ textFile         │
│ filter           │
│ map              │
└────────┬─────────┘
         │ Shuffle
         ▼
┌──────────────────┐
│ Stage 1          │
│ reduceByKey      │
└────────┬─────────┘
         │ Shuffle
         ▼
┌──────────────────┐
│ Stage 2          │  ← 绿色边框 = 正在运行
│ map              │
│ collect          │
└──────────────────┘
```

> **面试点**：面试官问"如何调试 Spark 性能问题"，第一反应就应该是 **Spark UI**。DAG 图一目了然——可以看到每个 Stage 有多少 Task、shuffle 多少数据、哪些 Task 耗时异常长。

#### 通过日志查看

```text
INFO DAGScheduler: Job 0 finished: collect at MyApp.scala:25, took 12.345 s
INFO DAGScheduler: Stage 0 (map at MyApp.scala:20) finished in 8.234 s
INFO DAGScheduler: Stage 1 (reduceByKey at MyApp.scala:21) finished in 3.456 s
```

> **踩坑经验**：开启 `spark.logLineage=true` 可以在日志中打印每个 RDD 的完整血统链，对调试非常有用。但这个日志非常长，生产环境慎用，建议只在测试环境调试时临时开启。

**窄依赖 vs 宽依赖—对比总结**：

| 对比维度 | 窄依赖 | 宽依赖 |
|---------|--------|--------|
| 分区关系 | 一对一或多对一 | 一对多 |
| 网络传输 | 不需要 | 需要 Shuffle |
| 执行方式 | Pipeline 执行 | 父 Stage 全完成再开始 |
| 分区恢复 | 只重算丢失分区 | 重算所有父分区 |
| 常见算子 | map, filter, flatMap, union | reduceByKey, groupByKey, join, repartition |
| 性能影响 | 低 | 高（主要瓶颈） |

## Task 类型

Stage 划分完成后，DAGScheduler 将每个 Stage 封装为一个 **TaskSet**，其中包含多个 **Task**（一个分区对应一个 Task）。

| Task 类型 | 对应 Stage | 说明 |
|-----------|-----------|------|
| **ShuffleMapTask** | ShuffleMapStage | 计算结果 → 按分区器写入 Shuffle 文件 |
| **ResultTask** | ResultStage | 执行 Action → 结果返回 Driver |

### Task 在 Executor 上的执行过程

```
ShuffleMapTask 执行流程：
1. 反序列化 Task 对象
2. 获取父 RDD 数据
3. 按照分区器将结果写入 Shuffle 文件
4. 返回 MapStatus（包含文件位置和大小）

ResultTask 执行流程：
1. 反序列化 Task 对象
2. 获取父 RDD 数据
3. 执行 Action 操作
4. 结果序列化返回 Driver
```

### Task 大小估算

```scala
// 一个 Task 处理的数据量 ≈ 分区大小
// 建议：每个 Task 处理 100MB~1GB 数据

// 分区数估算
val inputSize = 100 * 1024 * 1024 * 1024L  // 100GB
val blockSize = 128 * 1024 * 1024L          // HDFS 128MB
val defaultPartitions = inputSize / blockSize  // ~800 分区

// 经验公式
spark.conf.set("spark.sql.shuffle.partitions", "200")  // 默认
// 大集群建议：cores × 2~3
```

#### 分区数选择的经验法则

```scala
// 实际项目中如何设置分区数

// 1. CPU 密集型任务
val cpuCores = 100  // 集群总核数
val cpuPartitions = cpuCores * 2  // → 200 分区

// 2. IO 密集型任务
val inputGB = 100   // 输入数据量
val partitionSizeMB = 256  // 目标分区大小
val ioPartitions = (inputGB * 1024) / partitionSizeMB  // → 400 分区

// 3. 默认兜底
val defaultPartitions = spark.conf.get("spark.sql.shuffle.partitions")
// → 200（默认值，通常偏小，需要根据数据量调整）
```

**不同场景推荐分区数**：

| 场景 | 推荐值 | 原因 |
|------|-------|------|
| 少量数据（< 1GB） | 10-50 | 分区太多反而调度开销大 |
| 中等数据（1-100GB） | 100-500 | Task 处理量在 100MB-1GB |
| 大量数据（100GB-1TB） | 500-2000 | 充分利用集群并行度 |
| 超大量（> 1TB） | 2000-10000 | 需要 Executor 数量匹配 |

> **踩坑经验**：分区数太少 → 每个 Task 处理数据太多，容易 OOM。分区数太多 → 调度和序列化开销大，每个 Task 处理几 MB 数据还跑几秒的调度时间，得不偿失。
>
> 一个经典翻车案例：有个朋友处理 10MB 的小文件，没调分区数，默认 200 个分区，每个 Task 只处理 50KB 数据，结果 task 启动的序列化开销 > 实际计算时间，跑了几百个 Task 才完成——**性能比单机还差**。

## 数据本地性调度

### 为什么需要数据本地性？

在分布式系统中，**网络传输数据是最慢的操作之一**。如果 Task 所在的 Executor 和数据所在的节点是同一个，可以直接从本地磁盘读数据，速度远超跨网络读。Spark 会尽量让 Task 在数据所在的节点上执行。

### 五种级别

```
PROCESS_LOCAL  ───── 数据在同一个 JVM 中  ──── 最快
NODE_LOCAL     ───── 数据在同一个节点上    ──── 较快
NO_PREF        ───── 无位置偏好           ──── 
RACK_LOCAL     ───── 数据在同一个机架     ──── 较慢
ANY            ───── 跨网络传输           ──── 最慢
```

**各级别延迟对比**（典型值）：

| 级别 | 延迟 | 对比 |
|------|------|------|
| PROCESS_LOCAL | < 1ms | 进程内内存共享 |
| NODE_LOCAL | ~10ms | 跨 JVM 或本机磁盘 |
| NO_PREF | 可变 | 数据来自外部系统 |
| RACK_LOCAL | ~500μs-1ms 网络 + ~0.5ms 磁盘 | 同机架内网 |
| ANY | 1-10ms 或更高 | 跨机架/跨数据中心 |

### 本地性等待

**为什么需要等待？**

理想情况是所有 Task 都在 PROCESS_LOCAL 级别执行，但集群资源有限。如果某个 Executor 上所有 CPU 都被占满，新的 Task 要么等，要么在数据本地性上降级。

```scala
// Spark 不会立刻降级调度，而是等待一段时间
spark.locality.wait = 3s  // 默认 3s，可配

// 调度过程：
// 1. 优先 PROCESS_LOCAL → 如果没有空闲槽位 → 等待 3s
// 2. 降级到 NODE_LOCAL → 等待 3s
// 3. 降级到 RACK_LOCAL → 等待 3s
// 4. 最终 ANY
```

#### 本地性等待的优化

```scala
// 场景 A：数据量大、集群资源充足 → 增大等待时间
spark.locality.wait = 5s
// 更多 Task 能在同节点运行，减少网络传输

// 场景 B：集群资源紧张、长尾 Task 多 → 减小等待时间
spark.locality.wait = 1s
// 快速降级到 ANY，避免 Task 长时间等待导致 Job 超时

// 场景 C：流式作业（Structured Streaming）
spark.locality.wait = 0s  // 流处理追求实时性，不等
```

#### 如何通过 Spark UI 看本地性

在 Spark UI **Stages 页面**点击某个 Stage，查看 **Task Metrics** 的 **Locality Level** 列：

```
Task 1: PROCESS_LOCAL  ✅ 最优
Task 2: PROCESS_LOCAL  ✅
Task 3: NODE_LOCAL     ⚠️  次优
Task 4: RACK_LOCAL     ❌  大量此类表示资源不足
```

> **面试点**：数据本地性是 Spark 性能的关键。如果 Task 大量处于 RACK_LOCAL 或 ANY，说明集群负载高或 Executor 数量不够。常见的解决方案：
> 1. 增加 Executor 数量（扩大集群规模）
> 2. 减少每个 Executor 的并行 Task 数（`spark.executor.cores`）
> 3. 调整 `spark.locality.wait` 等待时间
> 4. 使用数据本地化调度策略（`spark.locality.wait.node` 等细分参数）

## 失败重试机制

```scala
// Task 级别
spark.task.maxFailures = 4  // 默认 4 次重试

// Stage 级别 — Shuffle 输出丢失
// 如果 Executor 宕机 → 其上的 Shuffle 输出丢失
// DAGScheduler 会重新提交该 Stage（重新计算所有分区）

// Job 级别 — 如果 Stage 重试失败 → Job 整体失败
```

### 多级容错机制

Spark 的容错分为三个层次，每一层失败后会向上层汇报：

```
Task 重试失败
  ↓ 报告给 DAGScheduler
重算该 Task 所在的 Stage（所有分区重算）
  ↓
Stage 重试失败
  ↓
Job 整体失败
  ↓
Application 抛出异常
```

#### Task 重试细节

```scala
// 重试配置
spark.task.maxFailures = 4          // 重试次数（含首次，即最多失败 3 次再重试 3 次）
spark.speculation = false           // 推测执行，默认关闭
spark.speculation.interval = 100ms  // 推测执行检测间隔
spark.speculation.multiplier = 1.5  // 推测执行触发倍数

// 推测执行：当一个 Task 比其他 Task 慢很多时，Spark 在另一个 Executor 上启动一个副本 Task
// 谁先完成就取谁的结果，慢的那个会被 kill
```

#### 哪些失败可以重试？

| 失败类型 | 能否重试 | 说明 |
|---------|---------|------|
| Task 代码异常 | 重试 | 随机性错误（网络抖动、临时 OOM）可能重试成功 |
| Executor 宕机 | 重试 | 新 Executor 启动后重新分配 Task |
| Shuffle 文件丢失 | 重试 | 重新提交上游 Stage 生成 Shuffle 文件 |
| Driver 宕机 | 不重试 | Application 整体失败 |
| 硬性限制（如内存不足） | 反复重试直到耗尽次数 | 需人工介入调参 |

### Shuffle 输出丢失的容错

```
Stage 0 (ShuffleMapStage): A → B → C
  ↓ 输出 Shuffle 文件到 Executor 本地磁盘

Executor 宕机 → 文件丢失
  ↓
DAGScheduler 检测到 → 重新提交 Stage 0
  ↓
只有丢失的分区会被重新计算（如果 Stage 0 已缓存则更快）
```

#### 为什么只重算丢失的分区？

DAGScheduler 内部维护了各个 Stage 的 **MapStatus**：

```scala
// MapStatus 记录了：
// - 每个 Task 输出了多少 Shuffle 文件
// - 文件存储在哪个 Executor
// - 文件大小

// 当 Executor 宕机 → DAGScheduler 知道该 Executor 上的哪些分区丢失
// 精确找到需要重算的分区，而不是整个 Stage
```

> **面试点**：这是 Spark 容错的一大优势：**精确恢复**。Hadoop MapReduce 如果 Task 失败，通常需要重新拉取全部数据。Spark 基于 RDD 依赖链，可以精确到分区级别进行重算，恢复效率远高于 MR。

#### 实战中的容错场景

**场景一：阶段性的 Executor 抖动**

```text
现象：某些 Task 偶尔失败，重试后成功
原因：瞬时 GC 停顿、网络抖动、磁盘 I/O 瓶颈
处理：通常是正常的，不需要调整。如果频繁出现，检查机器负载
```

**场景二：Shuffle 文件大规模丢失**

```text
现象：多个 Task 同时失败，提示 "Shuffle output file lost"
原因：Executor 批量宕机（如机器故障）
处理：系统自动重算，但如果数据量大且未 cache，非常耗时
```

**场景三：反复重试仍然失败**

```text
现象：同一个 Task 重试 4 次全部失败，Job 失败
原因：代码 bug 或数据质量问题（如空指针、非法字符）
处理：检查异常堆栈，修复代码或清洗数据
```

> **踩坑经验**：如果看到某个 Task 重试了 3 次都失败在第 4 次也失败了，**先看堆栈**。很多时候是数据本身的问题（如某个特殊字符导致解析异常）而不是集群问题。这时加再多重试也没用，需要修复代码或做数据清洗。

### 容错参数调优建议

```scala
// 生产环境推荐配置

// 重试次数（根据集群稳定性调整）
spark.task.maxFailures = 4      // 稳定集群
spark.task.maxFailures = 8      // 不稳定集群（如抢占式云实例）

// 推测执行（资源充足时开启）
spark.speculation = true        // 开启推测执行
spark.speculation.multiplier = 2.0  // 比中位数 Task 慢 2 倍时触发

// Shuffle 容错
spark.shuffle.service.enabled = true  // 开启 Shuffle Service
// Shuffle Service 使得 Executor 退出后 Shuffle 文件仍然可被读取
```

## 小结

| 组件 | 作用 |
|------|------|
| DAGScheduler | 将 RDD 依赖链转为 Stage DAG |
| TaskScheduler | 将 TaskSet 分发到 Executor |
| 窄依赖 | 同 Stage pipeline 执行 |
| 宽依赖 | Stage 边界，需要 Shuffle |
| 数据本地性 | 尽量让 Task 和数据在同一位置 |
| 失败重试 | Task 重试 4 次，Stage 丢失重算 |

### 面试高频问题速查

| 问题 | 答案要点 |
|------|---------|
| DAGScheduler 如何划分 Stage？ | 从后往前回溯，遇到宽依赖切分 |
| 窄依赖和宽依赖的区别？ | 分区对应关系、是否网络传输、pipeline/Stage 边界 |
| 如何查看 RDD 依赖？ | `rdd.dependencies` / Spark UI DAG |
| Task 分为哪两种？ | ShuffleMapTask（中间数据）、ResultTask（Action 结果） |
| 数据本地性级别？ | PROCESS_LOCAL → NODE_LOCAL → RACK_LOCAL → ANY |
| Task 失败如何恢复？ | 重试 4 次，Shuffle 丢失重算 Stage |
| 推测执行有什么用？ | 在另一个节点启动慢 Task 的副本，谁先完成用谁的结果 |

### 一句话总结

> **DAGScheduler 是 Spark 调度体系的"大脑"，它把用户代码翻译成一张有向无环图（DAG），然后按照宽依赖这张图切成一个一个 Stage，每个 Stage 再拆成多个 Task 交由集群执行——而这一切对用户是完全透明的。**
