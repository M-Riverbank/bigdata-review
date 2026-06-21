# Spark Shuffle 原理与调优

## Shuffle 是什么

### 为什么需要 Shuffle？

在分布式计算中，数据天然分散在各个节点上。当我们执行 `groupByKey`、`reduceByKey`、`join` 等操作时，**相同 key 的数据必须被拉到同一个分区**才能进行计算。这个"跨节点重新分组"的过程就是 `Shuffle`。

可以这样理解：假设全班同学按座位号坐在不同的教室（节点），现在老师要求"所有姓氏相同的人站在一起"——每个人就必须拿着自己的名牌走到指定位置，这个过程就是 `Shuffle`。

Shuffle 是分布式计算中**最昂贵的操作**——没有之一。它涉及**磁盘 I/O**、**网络传输**、**序列化/反序列化**和**内存排序**，往往是 Spark 作业的瓶颈所在。

```
没有 Shuffle:                         有 Shuffle:
┌──────────┐  ┌──────────┐           ┌──────────┐  ┌──────────┐
│ Node A   │  │ Node B   │           │ Node A   │  │ Node B   │
│ [1,a]    │  │ [2,b]    │           │ [1,a]    │  │ [2,b]    │
│ [1,c]    │  │ [2,d]    │           │ [1,c]    │  │ [2,d]    │
└──────────┘  └──────────┘           └─────┬────┘  └────┬─────┘
        直接计算                             │            │
                                            │  网络传输    │
                                       ┌────▼──┐    ┌───▼─────┐
                                       │ Node A│    │ Node B  │
                                       │ [1,a] │    │ [2,b]   │
                                       │ [1,c] │    │ [2,d]   │
                                       └───────┘    └─────────┘
```

### 哪些算子会触发 Shuffle？

> **面试点**：面试官经常问"Spark 中哪些算子会产生 Shuffle？"——记住这张清单可以加分。

| 算子 | Shuffle 原因 | 典型场景 |
|------|-------------|---------|
| `reduceByKey` | 相同 key 的数据必须汇聚到同一分区 | Word Count |
| `groupByKey` | 相同 key 的数据必须汇聚到同一分区 | 分组统计 |
| `join` | 两个 RDD/DF 按 key 配对 | 表关联 |
| `repartition` / `coalesce` | 调整分区数量 | 数据重分布 |
| `distinct` | 去重需要全局比较 | 唯一值统计 |
| `sortByKey` | 全局排序 | TopN |
| `intersection` / `subtract` | 集合运算需要比对 | 数据交集/差集 |

### Shuffle 的性能代价

Shuffle 的代价体现在四个维度：

1. **磁盘 I/O**：Map 端将数据溢写到磁盘，Reduce 端拉取后可能再次溢写
2. **网络传输**：数据跨节点传输，受带宽和延迟限制
3. **序列化/反序列化**：数据写入磁盘前序列化，读取后反序列化
4. **排序/聚合开销**：Map 端排序分区，Reduce 端可能再次排序

> **踩坑经验**：很多新手以为 Spark 是内存计算就没有磁盘操作。实际上，Shuffle 必然涉及磁盘溢写——**"内存计算"指的是计算环节在内存中，数据传输和排序依然要落盘**。

---

## Shuffle 演化

Spark 的 Shuffle 实现经历了多个版本的演进，从最初的 HashShuffle 到现在的 SortShuffle，**核心目标就是减少文件数、降低磁盘 I/O**。

### 1. HashShuffle（Spark 1.2 前，已淘汰）

这是 Spark 最早的 Shuffle 实现，思路简单粗暴——**为每个 Reducer 创建一个文件**。但简单带来的性能代价是巨大的。

```
问题：每个 Map Task 为每个 Reducer 创建独立文件
M 个 Map Task × R 个 Reducer = M × R 个文件

M=100, R=100 → 10000个小文件 → 磁盘IO爆炸
```

**哈希 Shuffle 的瓶颈分析：**

| 问题 | 说明 |
|------|------|
| 文件数爆炸 | 200 个 Map Task + 200 个 Reducer = 40,000 个文件 |
| 随机 I/O 瓶颈 | 大量小文件的随机写入远慢于连续写入 |
| 文件系统压力 | Linux 文件描述符数量有限，过多文件可能导致句柄泄漏 |
| 网络 Shuffle 开销 | 大量小文件的网络传输效率极低 |

> **面试点**：早期 Spark 版本（1.2 之前）在大规模集群上性能差，HashShuffle 产生海量小文件是核心原因之一。

### 2. SortShuffle（当前默认）

SortShuffle 的核心思想是**"Map 端先排序，再合并"**——每个 Map Task 只管写自己的数据，最后合并成一个文件 + 一个索引文件。

```
过程：
1. Map 端写入内存缓冲区
2. 缓冲区满后，先按 partitionId 排序，可能再按 key 排序
3. 溢写到磁盘（一个文件 + 索引文件）
4. 所有溢写文件合并（Merge）
5. Reduce 端按索引读取对应段

每 Map Task 只产生 2 个文件（data + index）!
```

**改进效果对比：**

| 指标 | HashShuffle | SortShuffle |
|------|-------------|-------------|
| 文件数 | M × R | M × 2 |
| 100M × 100R 文件数 | 10,000 | 200 |
| 磁盘 I/O 模式 | 随机小文件写入 | 连续写入 + 合并 |
| 排序开销 | 无 | 有（额外 CPU） |

**SortShuffle 的内部数据结构：**

```scala
// ShuffleInMemorySorter 的核心结构
// 每个 Map Task 维护一个指针数组，记录每条记录的 (partitionId, 偏移量)
// 当指针数组满了（默认 1MB 以上的 entry），触发排序 + 溢写

// 排序比较器：先比 partitionId，再比 key（按需）
// (partitionId1, key1) vs (partitionId2, key2)
// → partitionId 不同 → 小的在前
// → partitionId 相同且需要按 key 排序 → 比较 key
```

> **踩坑经验**：SortShuffle 虽然减少了文件数，但引入了排序开销。如果你的数据量不大（比如单节点几百 MB），排序的 CPU 消耗可能反而让作业更慢。在 Spark 3.x 中，小数据量场景会自动 fallback 到 BypassMergeSortShuffle。

### 3. UnsafeShuffle / TungstenSortShuffle

Tungsten 项目是 Spark 1.5 引入的内存管理优化，UnsafeShuffle 是其在 Shuffle 上的应用。它的核心是**直接在序列化后的二进制数据上操作，避免反序列化开销**。

```scala
// 启用条件：
// 1. 没有聚合操作（只是重新分区）
// 2. 序列化后的记录支持排序
// 3. 分区数 < 16777216

// 直接操作序列化后的二进制数据，无需反序列化
```

**TungstenSortShuffle 的优势：**

| 维度 | 普通 SortShuffle | TungstenSortShuffle |
|------|------------------|-------------------|
| 数据操作 | 需反序列化为 Java 对象 | 直接操作二进制 |
| GC 压力 | 大量中间对象 → GC 频繁 | 无中间对象，GC 友好 |
| CPU 利用率 | 序列化/反序列化耗 CPU | 减少序列化轮次 |
| 内存占用 | Java 对象有额外内存头 | 紧凑的字节数组 |

**什么场景下 TungstenSortShuffle 效果最好？**

```
1. 大 Shuffle 数据量（几百 GB+）→ GC 节省显著
2. 纯重分区（repartition）→ 没有聚合，满足条件
3. Kryo 序列化 → 序列化后记录长度固定，排序效率更高
```

### 4. BypassMergeSortShuffle

这是 SortShuffle 的一个优化变种。当**分区数少且没有聚合操作**时，"先排序再合并"就有些大材小用了——直接为每个分区写一个文件，最后合并就完事。

```scala
// 条件：分区数 < spark.shuffle.sort.bypassMergeThreshold（默认 200）
//       且没有 Map 端聚合

// 为每个分区创建一个文件，最后合并
// 适用于分区少 + 无聚合的场景
```

**什么时候 Bypass 机制会生效？**

```
假设 M = 1000, R = 50（分区数 50 < 200）

BypassMergeSortShuffle 流程：
1. 为 50 个分区分别写入数据（不需要排序！）
2. 最后合并为 1 个 data 文件 + 1 个 index 文件

和普通 SortShuffle 的区别：
- 普通版：先排序（按 partitionId）→ 溢写 → 合并
- Bypass 版：直接写分区文件 → 合并（跳过排序！）
```

> **面试点**：BypassMergeSortShuffle 跳过了排序步骤，在分区数少的场景下更高效。但它的条件限制很严格——**分区数必须小于 200（可配置）且不能有 map 端聚合**。

**四种 Shuffle 机制的选择流程：**

```
              ┌──────────────────────────────────────┐
              │         Shuffle 操作触发              │
              └──────────┬───────────────────────────┘
                         │
              ┌──────────▼──────────┐
              │ 有 map 端聚合吗？    │
              └────┬─────┬─────────┘
                  Yes    No
                   │      │
          ┌────────▼┐  ┌──▼──────────────────┐
          │ 普通    │  │ 分区数 < 200 ？      │
          │ Sort   │  └──┬──────────┬────────┘
          │ Shuffle│   Yes          No
          └────────┘     │            │
                   ┌─────▼──┐  ┌─────▼──────────┐
                   │Bypass  │  │ 满足 Tungsten   │
                   │Merge   │  │ 条件？           │
                   │Sort    │  └──┬──────┬───────┘
                   └────────┘   Yes      No
                                 │        │
                          ┌──────▼─┐ ┌───▼────────┐
                          │Tungsten│ │ 普通 Sort  │
                          │Sort    │ │ Shuffle    │
                          └────────┘ └────────────┘
```

---

## Shuffle 写流程详解

Shuffle Write（Map 端）是整个 Shuffle 的第一阶段，**Map Task 需要将数据按分区号排序并写入磁盘**，等待 Reduce 端来拉取。

### 写流程全貌

```
Map Task Shuffle Write:

1. 数据写入 ShuffleExternalSorter
   └→ 积累到一定量 → spill 到磁盘
      ├→ 按 (partitionId, key) 排序
      └→ 每个 spill 一个文件

2. spill 文件之间 merge
   └→ 最终合并为一个 sorted 文件
      ├→ data file: [partition0 records][partition1 records]...
      └→ index file: 记录每个 partition 的 offset
```

### 写流程的详细步骤

```scala
// 步骤 1：ShuffleExternalSorter 接收数据
// Map Task 每输出一条记录，序列化后放入 sorter 的数据页（data page）
// 同时记录 (partitionId, 记录地址) 到指针数组

// 关键数据结构
// - data page: 存储序列化后的记录
// - pointer array: 存储 (encodedPartitionId | recordAddress) 对

// 步骤 2：触发溢写（Spill）
// 当 pointer array 用完或数据页用完时 → 开始溢写
// 按照 partitionId 排序 pointer array → 按顺序写入磁盘

// 步骤 3：合并（Merge）
// 将多个 spill 文件合并为 1 个文件
// 使用优先队列（PriorityQueue）多路归并
// 合并后每个 partition 的数据在文件中是连续的
```

### 写缓冲区调优

| 参数 | 默认值 | 作用 | 调优建议 |
|------|--------|------|---------|
| `spark.shuffle.file.buffer` | 32K | Map 端写缓冲区大小 | 增大到 64K-128K 可减少磁盘 I/O 次数 |
| `spark.shuffle.spill.batchSize` | 10000 | 每次 spill 批量处理记录数 | 适当增大减少 spill 频率 |

**Spill 的触发时机：**

```scala
// 当 sorter 中的数据量超过可用执行内存的 threshold 时触发 spill
// 频繁 spill = 多次磁盘写入 = 性能下降

// 理想状态：没有 spill，数据全部在内存中完成排序
// 现实情况：大 Shuffle 基本都会 spill，关键是控制 spill 次数
```

> **踩坑经验**：很多人以为 `spark.shuffle.memoryFraction = 0.2` 就是 Shuffle 能用 20% 的 JVM 堆内存。实际上这是在 execution memory pool 中切割出来的。如果你的 JVM 堆设得不够大（比如 1GB），Shuffle 可用内存只有 200MB，大 Shuffle 必然频繁 spill 到磁盘。

### Map 端的数据流

```
Map Task 输出
     │
     ▼
AppendOnlyMap 或 ExternalSorter
     │
     ├── 数据在内存中积累
     ├── 达到阈值 → 排序 → 溢写（spill）到磁盘
     └── 所有数据写完 → 多路归并合并所有 spill 文件
               │
               ▼
    最终输出：1 个 data 文件 + 1 个 index 文件
               │
               ▼
    物理文件位于：spark.local.dir 配置的目录中
```

---

## Shuffle 读流程详解

Shuffle Read（Reduce 端）是 Shuffle 的第二阶段，**Reduce Task 需要从各个 Map Task 的输出中拉取属于自己的分区数据**。

### 读流程全貌

```
Reduce Task Shuffle Read:

1. 从 MapOutputTracker 获取所需 block 位置

2. 对每个 Map Task 的输出：
   ├→ 远程拉取 → fetch 线程池（默认 5 个线程）
   ├→ 本地读取 → 直接读文件
   └→ 放入内存 (reduce 端缓冲区)

3. 内存满了 → 溢写到磁盘 (ExternalSorter)

4. 所有数据拉取完后 → merge + sort → 给用户算子
```

### 读流程的详细步骤

```scala
// 步骤 1：获取输出位置元信息
// MapOutputTrackerMaster（Driver 端）保存每个 Map Task 输出的 block 位置
// Reduce Task 通过 MapOutputTrackerWorker 向 Driver 请求这些信息

// 步骤 2：确定拉取策略（本地 vs 远程）
// 对于每个 Map Task 的输出：
// - 如果 Map Task 在同一个 Executor → 本地读取（LocalBlockFetcher）
// - 如果 Map Task 在其他 Executor → 网络拉取（RemoteBlockFetcher）

// 步骤 3：网络拉取细节
// 使用 Netty 传输层，默认 5 个并发 fetch 线程
// 每个线程一次拉取的数据量不超过 spark.reducer.maxSizeInFlight / 并发数

// 步骤 4：数据合并
// 拉取到的块放入 ShuffleMergedBlock 或直接喂给 ExternalSorter
// 如果数据量小（< spark.shuffle.memoryFraction），全在内存中做 merge
// 数据量大 → 先 spill 到磁盘，再多路归并
```

### 拉取机制的关键点

```scala
// 拉取过程的特点：

// 1. 边拉取边聚合（对于 reduceByKey 等聚合操作）
//    → 减少内存中缓存的数据量

// 2. 拉取超时重试机制
//    spark.shuffle.io.maxRetries = 3
//    spark.shuffle.io.retryWait = 5s
//    → 3 次重试，每次间隔 5 秒，全失败则抛出 FetchFailedException

// 3. 黑名单机制
//    → 如果某个 Executor 连续失败，会被加入黑名单
//    → 避免反复向故障节点拉取数据
```

### 本地读取 vs 远程读取

| 对比维度 | 本地读取 | 远程读取 |
|---------|---------|---------|
| 传输路径 | 磁盘 → 进程内 | 磁盘 → 网络 → 目标节点内存 |
| 速度 | 快（GB/s 级别） | 慢（百 MB/s 级别，受网络限制） |
| 资源消耗 | 无网络消耗 | 占用网络带宽 + 源节点 CPU |
| 失败概率 | 低 | 高（网络抖动、节点故障） |

> **面试点**：Spark 在调度时会有**数据本地性（Data Locality）** 的考虑——尽量将 Reduce Task 调度到数据所在节点。`spark.locality.wait` 参数控制等待本地数据的最长时间（默认 3 秒），超时后会调度到远程节点。

### Reduce 端缓冲区调优

| 参数 | 默认值 | 作用 | 调优建议 |
|------|--------|------|---------|
| `spark.reducer.maxSizeInFlight` | 48M | 每个 reduce task 同时拉取的数据量上限 | 增大到 96M-256M 可减少拉取轮次 |
| `spark.shuffle.io.maxRetries` | 3 | 拉取失败重试次数 | 大集群增加到 6-10 |
| `spark.shuffle.io.retryWait` | 5s | 重试等待时间 | 网络不稳定时可加大到 10-30s |
| `spark.reducer.maxReqsInFlight` | Int.MaxValue | 并发拉取请求数上限 | 建议限制为 64-128 避免网络风暴 |

---

## 关键配置参数

Shuffle 的性能调优本质上是在**时间换空间**和**空间换时间**之间做权衡——增大缓冲区可以减少磁盘 I/O，但会增加内存压力；反之，减小缓冲区可以省内存，但会增加 spill 次数。

### Map 端

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.shuffle.file.buffer` | 32K | Map 端写缓冲区大小 |
| `spark.shuffle.spill.batchSize` | 10000 | 每次 spill 的记录数 |

**实际操作建议：**

```scala
// 大 Shuffle 场景调优示例
spark.conf.set("spark.shuffle.file.buffer", "64k")   // 增大写缓冲区
spark.conf.set("spark.shuffle.spill.batchSize", "20000") // 减少 spill 批次

// 注意：文件缓冲区太大也会有问题
// → 每个 Map Task 都有独立的缓冲区
// → 同时运行 N 个 Map Task，总共占用 N × buffer 大小
// → 比如 100 个并发 Map Task，buffer 设为 1MB，就要 100MB
```

### Reduce 端

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.reducer.maxSizeInFlight` | 48M | 每个 reduce task 同时拉取的数据量上限 |
| `spark.shuffle.io.maxRetries` | 3 | 拉取失败重试次数 |
| `spark.shuffle.io.retryWait` | 5s | 重试等待时间 |

**实际场景调优：**

```scala
// 场景 1：网络带宽充足，Reduce 端内存够
spark.conf.set("spark.reducer.maxSizeInFlight", "256m")
// 增大会减少拉取轮次，加速 Shuffle 读

// 场景 2：集群网络不稳定，大作业容易失败
spark.conf.set("spark.shuffle.io.maxRetries", "10")
spark.conf.set("spark.shuffle.io.retryWait", "30s")
// 增加重试次数和等待时间，提升作业稳定性
```

### 内存

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.shuffle.memoryFraction` | 0.2 | Shuffle 使用的执行内存占比 |
| `spark.shuffle.sort.bypassMergeThreshold` | 200 | bypass 机制的分区阈值 |

**Shuffle 内存核算：**

```
JVM 堆内存 = 300MB（示例）
  ├── Reserved Memory = 300MB × 0.2 = 60MB（系统保留）
  ├── Spark Memory = 300MB × 0.6 = 180MB（spark.memory.fraction）
  │   ├── Execution Memory = 180MB × 0.5 = 90MB（spark.memory.storageFraction）
  │   │   └── Shuffle Memory = 90MB × 0.2 = 18MB ← 实际用于 Shuffle 的内存！
  │   └── Storage Memory = 90MB（缓存等）
  └── User Memory = 300MB × 0.2 = 60MB（用户代码用）
```

> **踩坑经验**：很多人的 Shuffle 慢是因为**堆内存设置太小**。从上面算出来，300MB 堆下 Shuffle 只有 18MB——只要 Shuffle 数据量大一点就疯狂 spill。建议至少给 Executor 分配 4-8GB 堆内存。

### 其他实用配置

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `spark.shuffle.service.enabled` | false | 是否启用外部 Shuffle Service |
| `spark.shuffle.service.port` | 7337 | Shuffle Service 端口 |
| `spark.shuffle.registration.timeout` | 5000ms | Shuffle Service 注册超时 |
| `spark.shuffle.sort.initialBufferSize` | 4096 | SortShuffle 初始排序缓冲区大小 |

**外部 Shuffle Service 的作用：**

```
Executor 挂掉时，它的 Shuffle 数据还在磁盘上吗？
→ 没有 Shuffle Service：Executor 挂 = Shuffle 数据丢 = 重算
→ 有 Shuffle Service：单独守护进程管理 Shuffle 数据，Executor 挂了数据还在

适用场景：Dynamic Allocation（动态分配 Executor）
→ Executor 被回收后，别的 Task 还能拉取它的 Shuffle 输出
```

---

## 数据倾斜处理（面试必考！）

数据倾斜是 Spark 面试中**最高频的问题之一**，也是在生产环境中最常见的性能杀手。**一个分区处理了 99% 的数据，其他分区只处理 1%**，整个 Stage 等这个慢 Task 完成。

### 倾斜诊断

```scala
// 在 Spark UI 中查看：
// 1. Stage 详情页 → 各 Task 执行时间
//    - 大部分 Task 很快（秒级）→ 个别 Task 很慢（分钟级）↔ 数据倾斜
// 2. SQL 页 → Join/GroupBy 算子
//    - 查看每个 partition 的数据量

// 代码方式定位
df.groupBy("key").count().orderBy(desc("count")).show(20)
```

**如何快速判断是数据倾斜还是计算逻辑慢？**

| 现象 | 数据倾斜 | 计算逻辑慢 |
|------|---------|-----------|
| 部分 Task 执行时间远大于其他 | 是 | 可能 |
| 所有 Task 执行时间都长 | 否 | 是 |
| 个别 Task 频繁 OOM | 是 | 否 |
| 数据分布明显不均匀 | 是 | 否 |
| 提高并行度后缓解 | 是 | 否 |

### 解决方案 1：两阶段聚合（Key 加盐）

这是解决聚合操作（`groupByKey`、`reduceByKey`）数据倾斜的**首选方案**。核心思想是对 key 加随机前缀，将原来一个热 key 拆成多个子 key，让计算分散到多个 Task。

```scala
val skewedRDD: RDD[(String, Long)] = ...

// 第一轮：加随机前缀聚合（每个 key 被拆分）
val saltRDD = skewedRDD.map { case (k, v) =>
  val salt = Random.nextInt(100)
  (s"${salt}_$k", v)
}.reduceByKey(_ + _)

// 第二轮：去掉前缀再聚合
val result = saltRDD.map { case (sk, v) =>
  val key = sk.substring(sk.indexOf("_") + 1)
  (key, v)
}.reduceByKey(_ + _)
```

**加盐的注意事项：**

```
1. 粒度选择：Random.nextInt(100) 中的 100 是分区数
   - 太小 → 倾斜依然存在
   - 太大 → 小分区过多，合并开销大
   - 推荐值：分区数 × 10 或最大热 key 的记录数

2. 两次 Shuffle：
   - 第一次加盐聚合 = 1 次 Shuffle
   - 第二次去盐聚合 = 1 次 Shuffle
   - 总共 2 次 Shuffle，性能有代价
   - 但比起一个 Task 跑几十分钟，2 次 Shuffle 完全可以接受

3. 局限性：
   - 仅适用于聚合操作（reduceByKey、groupByKey）
   - 不适用于 Join 操作
   - Join 需要其他方案
```

### 解决方案 2：Map Join

当一个大表 Join 一个小表时，最优方案是**将小表广播到所有节点**，避免 Shuffle。

```scala
// 小表 < broadcast 阈值（默认 10MB）
val smallDF = spark.read.parquet("dim_table").cache()

import org.apache.spark.sql.functions.broadcast
largeDF.join(broadcast(smallDF), "key")  // 显式 broadcast hint
```

**Broadcast Join 的适用条件：**

| 条件 | 说明 |
|------|------|
| 小表大小 | < `spark.sql.autoBroadcastJoinThreshold`（默认 10MB） |
| 数据量 | 千行到百万行级别 |
| 典型场景 | 维度表 Join 事实表、配置表 Join 业务表 |

**为什么不直接设大广播阈值？**

```scala
// 有人会想：把阈值设到 1GB 不就行了？
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "1073741824")

// 但广播是把整张表复制到每个 Executor
// 100 个 Executor × 1GB = 100GB 网络传输！
// 而且每个 Executor 多占 1GB 内存
// 物极必反——广播太大反而导致 OOM 或网络风暴
```

> **面试点**：面试官常问"大表 Join 大表怎么办？"——先问"能过滤吗？能转化为大表 Join 小表吗？"很多场景下通过提前过滤可以将数据量大幅减少。

### 解决方案 3：倾斜 Join（拆分处理）

当大表 Join 大表且某个 key 数据量特别大时，需要对**倾斜的 key 单独处理**。

```scala
// 识别倾斜的 key
val skewedKeys = df.groupBy("key").count()
  .filter(col("count") > threshold)
  .select("key").collect().map(_.getAs[String]("key"))

// 对倾斜 key 加盐
val skewedDF = df.filter(col("key").isin(skewedKeys: _*))
  .withColumn("salt_key", concat(col("key"), lit("_"), rand(100).cast("int")))

val nonSkewedDF = df.filter(!col("key").isin(skewedKeys: _*))
// 对倾斜部分做加盐 Join，常规部分做普通 Join，最后 union
```

**完整实现：**

```scala
// 假设有两张大表：orders 和 order_items，按 order_id Join
// 如果某个 order_id（比如大促活动）有上千万条 item

// 第 1 步：定位倾斜 key
val skewThreshold = 1000000 // 100 万条记录
val skewOrderIds = orders
  .groupBy("order_id").count()
  .filter(col("count") > skewThreshold)
  .select("order_id")
  .as[String].collect().toSet

// 第 2 步：将数据分为倾斜集和正常集
val skewOrders = orders.filter(col("order_id").isinIn(skewOrderIds.toSeq: _*))
val normalOrders = orders.filter(!col("order_id").isinIn(skewOrderIds.toSeq: _*))

val skewItems = order_items.filter(col("order_id").isinIn(skewOrderIds.toSeq: _*))
val normalItems = order_items.filter(!col("order_id").isinIn(skewOrderIds.toSeq: _*))

// 第 3 步：对倾斜数据加盐 Join
val saltedSkewJoin = skewOrders
  .withColumn("salt", (rand() * 100).cast("int"))
  .join(
    skewItems.withColumn("salt", (rand() * 100).cast("int")),
    Seq("order_id", "salt")
  )

// 第 4 步：正常数据无需加盐，直接 Join
val normalJoin = normalOrders.join(normalItems, "order_id")

// 第 5 步：合并结果
val result = saltedSkewJoin.union(normalJoin)
```

### 解决方案 4：调整并行度

有时候问题没那么复杂——就是**分区数太少**导致数据分布不均，调大分区数就能缓解。

```scala
// 第一种：spark.default.parallelism
spark.conf.set("spark.default.parallelism", "400")  // 通常是 cores × 2~3

// 第二种：显式指定分区数
rdd.reduceByKey(_ + _, 400)
df.groupBy("key").agg(sum("val")).repartition(400)
```

**并行度设置的黄金法则：**

```
并行度 = 每个 Executor 的核心数 × Executor 数量 × （2 ~ 3）

举个例子：
20 个节点 × 每个节点 8 核 = 160 核
推荐并行度 = 160 × 2 = 320（或 160 × 3 = 480）

为什么要多 2-3 倍？
→ 因为 Task 执行时间有差异，多个 Task 可以"填满"空闲核心
→ 也叫 "CPU 利用率优化"
```

> **踩坑经验**：并行度不是越大越好。并行度 10000 意味着 10000 个 Task，每个 Task 的开销（调度、序列化）累加起来非常可观。一般生产环境控制在 500-2000 就够用了。

### 解决方案 5：过滤异常数据

有时候数据倾斜是因为**脏数据**——比如 key 为 null 或者一些根本没有意义的 key 占据了大量数据。

```scala
// 第 1 步：查看 key 分布
df.groupBy("key").count()
  .orderBy(desc("count"))
  .show(20)

// 第 2 步：如果发现 null key 占了 90% 的数据
val cleaned = df.filter(col("key").isNotNull)
// 或者 null key 有自己的业务含义，单独处理
val nullKeyData = df.filter(col("key").isNull)
val validData = df.filter(col("key").isNotNull)
```

### 各方案对比总结

| 方案 | 适用场景 | 优点 | 缺点 |
|------|---------|------|------|
| 两阶段聚合 | groupBy/reduceByKey | 实现简单，效果明显 | 2 次 Shuffle，不适用于 Join |
| Map Join | 大表 Join 小表 | 完全避免 Shuffle | 小表必须能广播 |
| 倾斜 Join | 大表 Join 大表 + 少量倾斜 key | 针对性强 | 实现复杂，需要代码改动 |
| 调大并行度 | 轻微不均匀分布 | 一行配置 | 治标不治本 |
| 过滤异常数据 | 脏数据导致的倾斜 | 简单有效 | 需要确认数据是否可过滤 |

---

## 小结

| 层面 | 核心优化策略 |
|------|-------------|
| Map 端 | 增大 spark.shuffle.file.buffer、用 Bypass 机制 |
| Reduce 端 | 控制 `maxSizeInFlight`、增加 fetch 线程数 |
| 内存 | shuffle.memoryFraction 调优 |
| 数据倾斜 | 两阶段聚合、Map Join、加盐 Join、调并行度 |
| 文件数 | SortShuffle 每个 Map Task 就 2 个文件 |

### 面试高频题速查

> **面试点 1：Spark 为什么慢？**
> 答：大部分情况下是 Shuffle 慢。Shuffle 涉及磁盘 I/O、网络传输、序列化/反序列化、排序——每一步都有开销。

> **面试点 2：数据倾斜怎么定位？**
> 答：看 Spark UI 的 Stage 详情——个别 Task 执行时间远超平均；或者跑 `df.groupBy("key").count().orderBy(desc).show()` 查看 key 分布。

> **面试点 3：reduceByKey 和 groupByKey 哪个好？为什么？**
> 答：`reduceByKey` 有 map 端预聚合（combine），Shuffle 数据量小；`groupByKey` 不预聚合，所有数据原样传输。能用 `reduceByKey` 就不用 `groupByKey`。

> **面试点 4：Shuffle 产生的文件数怎么算？**
> 答：SortShuffle 下 = 2 × Map Task 数。HashShuffle（已淘汰）下 = M × R。所以文件数大减。

### 核心原则

```
Shuffle 优化的三条黄金法则：
1. 尽量少 Shuffle（用 Map Join 替代 Reduce Join）
2. 尽量早 Shuffle（先过滤再 Shuffle，减少数据量）
3. 尽量均匀 Shuffle（解决数据倾斜）
```

> **踩坑经验**：最后送大家一句话——**"先看 Spark UI，再谈优化"**。不要上来就调参数，先确认瓶颈在 CPU、磁盘 I/O、网络还是内存，对症下药才有效。
