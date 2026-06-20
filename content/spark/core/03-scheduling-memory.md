# Spark 任务调度与内存管理

## Spark 运行时架构

```
┌──────────────────────────────────────────────────────┐
│                     Driver                           │
│  ┌───────────┐  ┌────────────┐  ┌────────────────┐  │
│  │ SparkContext│  │ DAGScheduler│  │ TaskScheduler  │  │
│  └───────────┘  └──────┬─────┘  └──────┬─────────┘  │
│                        │               │             │
└────────────────────────┼───────────────┼─────────────┘
                         │               │
              ┌──────────▼───────┐      │
              │ Cluster Manager  │      │
              │ (YARN/K8s/Mesos) │      │
              └──────────┬───────┘      │
                         │              │
        ┌────────────────┼──────────────┼──────────────┐
        │                │              │              │
┌───────▼──┐      ┌──────▼──┐    ┌──────▼──┐   ┌─────▼───┐
│ Executor │      │ Executor │    │ Executor │   │ Executor│
│ ┌──────┐ │      │ ┌──────┐ │    │ ┌──────┐ │   │ ┌──────┐│
│ │ Task │ │      │ │ Task │ │    │ │ Task │ │   │ │ Task ││
│ │ Task │ │      │ │ Task │ │    │ │ Task │ │   │ │ Task ││
│ └──────┘ │      │ └──────┘ │    │ └──────┘ │   │ └──────┘│
└──────────┘      └──────────┘    └──────────┘   └─────────┘
```

### Driver 职责

1. 将用户代码翻译为 DAG（DAGScheduler）
2. 依据 Shuffle 依赖划分 Stage
3. 将 Stage 以 TaskSet 提交给 TaskScheduler
4. 调度 Task 到 Executor 执行
5. 跟踪 Task 执行状态，处理失败重试

### Executor 职责

1. 接收并执行 Task
2. 向 Driver 汇报 Task 结果
3. 提供内存 / 磁盘存储（BlockManager）
4. 每个应用独享 Executor，相互隔离

## DAGScheduler 详解

### Job → Stage → Task

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

### Stage 内部

```scala
// 一个 Stage 内部的 Task 数量 = 该 Stage 最后一个 RDD 的分区数

// Stage 0: textFile → map
//   textFile 有 HDFS 输入分片数 → N 个 Task
//   → 每个 Task 处理一个 HDFS Block

// Stage 1: reduceByKey → collect
//   reduceByKey 的分区数 = spark.default.parallelism
//   → P 个 Task（通常 ≠ N）
```

### 失败处理

```scala
// DAGScheduler 的容错策略：
// 1. 一个 Task 失败 → 重试（默认 4 次，spark.task.maxFailures）
// 2. 一个 Stage 失败 → 重新提交整个 Stage
// 3. ShuffleMapStage 输出丢失 → 重新计算父 Stage
```

## TaskScheduler 详解

```scala
// TaskScheduler 负责：
// 1. 将 TaskSet 中的 Task 分发到 Executor
// 2. 处理 Task 失败时通知 DAGScheduler
// 3. 返回 Task 执行结果给 DAGScheduler

// 调度策略（两种）：
// FIFO（默认）：先入先出，适用于单用户
spark.scheduler.mode = FIFO

// FAIR：多 Job 公平共享资源
spark.scheduler.mode = FAIR
```

### 数据本地性（Data Locality）

```
优先级（从高到低）：
1. PROCESS_LOCAL — 数据在同一 JVM 中
2. NODE_LOCAL    — 数据在同一节点
3. NO_PREF       — 无位置偏好（如从 DB 读取）
4. RACK_LOCAL    — 数据在同一机架
5. ANY           — 任意位置（需跨网络）

spark.locality.wait = 3s（等待更高本地性的最长时间）
```

> 数据显示在面前都不取 → 等 3s → 降级到下一级 → 再次等待 → 最终降级到 ANY

## Spark 内存管理

### 1.6 前：静态内存管理

```
堆内存总量
├── Storage（缓存 RDD）: 60% × (1 - 0.2) = 48%
│   └── 不可借用
├── Execution（Shuffle）: 20% × (1 - 0.2) = 16%
│   └── 不可借用
└── Other: 20%
```

### 1.6+ 统内存管理（Unified Memory Manager）

```
堆内存总量 = spark.executor.memory（例如 4G）

Reserved Memory = 300MB（系统保留）

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

```
┌─────────────────────────────────────────────┐
│              JVM Heap (4G)                   │
│  ┌───────────────────────────────────────┐  │
│  │     Reserved (300MB)                   │  │
│  ├────────────┬──────────────────────────┤  │
│  │ User Mem   │    Spark Memory           │  │
│  │ (40%)      │  ┌──────────┬──────────┐ │  │
│  │            │  │ Storage  │Execution │ │  │
│  │  ~1.48G    │  │ (50%)   │ (50%)    │ │  │
│  │            │  │ ~1.11G   │ ~1.11G   │ │  │
│  │            │  └──────────┴──────────┘ │  │
│  └────────────┴──────────────────────────┘  │
└─────────────────────────────────────────────┘
```

### 堆外内存

```scala
spark.memory.offHeap.enabled = true  // 开启堆外内存
spark.memory.offHeap.size = 2g       // 堆外大小

// 堆外内存优势：
// 1. 不受 JVM GC 影响
// 2. 避免 GC 导致的暂停
// 3. 更适合 Tungsten 的二进制格式

// 堆外内存劣势：
// 1. 不归 JVM 管理，可能内存泄漏
// 2. 需要额外的序列化/反序列化
```

## 面试高频考点

### Q: Spark Job / Stage / Task 的关系？

- **Job**：一个 Action 产生一个 Job
- **Stage**：Job 内按宽依赖划分 Stage（Shuffle 为界）
- **Task**：Stage 内按分区执行的运算单元，一个分区一个 Task

### Q: 为什么 Spark Task 是多线程的而 Hadoop MR Task 是多进程的？

多线程的 Task 共享 Executor JVM，启动快、内存省，但隔离差。Hadoop MR 每个 Task 是独立 JVM 进程，隔离好但启动慢。Spark 的 Executor 一次性启动，Task 复用线程池。

### Q: 为什么要先调度 PROCESS_LOCAL 数据？

网络传输是分布式计算的瓶颈。让 Task 在数据所在的节点上执行，避免跨网络的数据传输。数据本地性是 Hadoop→Spark 时代最重要的优化策略之一。

### Q: Executor 心跳超时怎么办？

如果在 YARN 上：
1. 增大 `spark.executor.heartbeatInterval`（默认 10s）
2. 增大 `spark.network.timeout`（默认 120s）
3. 检查是否有 GC 长暂停（Executor 被 GC 占用 → 无法发心跳 → 被标记死亡）

### Q: shuffle.memoryFraction 调到多少合适？

默认 0.2（1.6 以前）。如果 Shuffle 数据量大，可以调大；如果缓存数据多，调小。在统一内存管理下，Storage 和 Execution 可动态借用，通常不需要手动调整。

## 小结

| 组件 | 关键职责 |
|------|---------|
| DAGScheduler | Job → Stage → TaskSet |
| TaskScheduler | Task → Executor（考虑数据本地性） |
| 数据本地性 | PROCESS_LOCAL > NODE_LOCAL > RACK_LOCAL > ANY |
| 统一内存 | Storage/Execution 动态借用，Execution 优先 |
| Worker/Task | Executor 管多核，每个核一个 Task Slot |
