# Spark SQL — 自适应查询执行 (AQE)

## 为什么需要 AQE

在 Spark 3.0 之前，查询计划完全在**编译期**确定。这意味着 Spark 在做优化时只能依赖**静态的统计信息**（比如表的大小、列的唯一值数量）。但实际运行时，这些静态信息可能和真实数据分布大相径庭：

```
编译期确定的计划：
  └── 分区数 200、Join 策略 SMJ、没有倾斜处理
  └── 如果实际数据分布和预估不同→执行计划不是最优的
  └── 典型案例：
       - 预估大表 Join，实际 Shuffle 后小表 < 10MB → 应该 Broadcast
       - 200 个分区 Shuffle 后每分区只有 1MB → 分区太多
       - 某个 key 数据量巨大 → 数据倾斜
```

AQE 正是为了解决这个"编译期拍脑袋"的问题。

> **面试点**：AQE（Adaptive Query Execution）最核心的思想是"运行时优化"——让 Spark 在查询执行过程中，根据实际数据分布来调整后续的执行计划。这就像开车导航：你出发前规划了一条路线（编译期计划），但如果路上遇到堵车，导航会根据实时路况重新规划路线（AQE）。

### AQE 的解决思路

AQE 将整个查询拆分为多个 Stage，每个 Stage 之间由 Shuffle 分隔。每个 Stage 执行完成后，Driver 会收集执行统计信息（各个分区的大小），然后根据这些统计信息动态调整后续 Stage 的执行计划。

```
传统 Spark 查询：编译期决定 → 一路执行到底
AQE Spark 查询：编译期决定 → Stage 1 执行 → 收集统计 → 动态调整 Stage 2 → ...

关键洞察：Shuffle 是天然的 "检查点"
  每个 Shuffle 都要把数据写到磁盘
  写完之后，Driver 就能精确知道每个分区的大小
  有了精确的大小信息，就能做出更优的决策
```

## AQE 三大核心优化

```scala
// 启用 AQE（Spark 3.2+ 默认开启）
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### 1. 动态合并 Shuffle 分区

这是 AQE 最直观的优化效果。

```
问题：shuffle.partitions=200，实际数据量很小
  → 200 个分区每分区只有 1MB，空跑 200 个 Task
  → Task 调度开销 > 计算开销

AQE 处理：
  Shuffle 之后统计每个分区大小
  发现平均只有 1MB → 自动合并为 10 个分区
  → 只有 10 个 Task，调度开销大大减少
```

```scala
// 影响动态合并的参数
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.minPartitionNum", "10")  // 最少分区
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB")      // 目标分区大小
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `coalescePartitions.enabled` | true (3.2+) | 动态合并开关 |
| `minPartitionNum` | default Spark shuffle partitions | 合并后最少分区数 |
| `advisoryPartitionSizeInBytes` | 64MB | 目标分区大小，合并时参考 |

> **踩坑经验**：动态合并的效果取决于 `advisoryPartitionSizeInBytes`。如果设置的太大（比如 256MB），合并后的分区数据量可能超过单个 Task 的内存容量，导致溢写磁盘或者 OOM。如果设置的太小（比如 16MB），合并效果不明显。建议根据集群配置和数据类型，在 64MB-256MB 之间选择合适的值。

### 2. 动态 Join 策略

编译期决定的 Join 策略不一定是最优的——但 AQE 可以在运行时纠正。

```
问题：编译期决定 SortMergeJoin，实际小表可以 Broadcast

AQE 处理：Shuffle 完小表数据后，发现可以广播
  → 动态切换为 BroadcastHashJoin
  → 大表不需要再 Shuffle，大幅减少网络传输
```

```scala
// 编译期计划：SortMergeJoin
// AQE 在运行时检测到右表 < 10MB
// 自动转为：BroadcastHashJoin

// 这意味着不需要手动写 broadcast hint
// AQE 会自动判断
```

动态 Join 策略的切换流程：

```
Stage 1：Shuffle 两张表
  → 数据写入磁盘
  → Driver 收集分区大小

判断：小表实际大小 < autoBroadcastJoinThreshold (10MB)？
  → 是：将后续的 SortMergeJoin 替换为 BroadcastHashJoin
  → 否：保持 SortMergeJoin，但可以使用动态合并

如果切换为 BHJ：
  → 大表剩余的 Shuffle 可以被取消（被广播的表不需要 Shuffle）
  → 大表直接在本地读取，做 Map 端 Join
```

> **面试点**：动态 Join 策略为什么能提升性能？因为编译期 Spark 只能根据表级别的统计信息（总行数）做判断，但 AQE 在 Shuffle 后能知道**每个分区**的实际数据量。如果 Shuffle 后发现某侧的数据很小（即使整表很大，但 WHERE 过滤后很小），就可以切换到 BHJ，省掉大表的 Shuffle。

### 3. 动态处理数据倾斜

数据倾斜是大数据领域最令人头疼的问题之一，AQE 提供了自动化解决方案。

```
问题：大表 Join，某个 key 占 50% 数据
  → 一个 Task 处理一半数据，其他 Task 几秒完成，这个 Task 跑 30 分钟
  → 整个 Stage 被这一个 Task 拖慢

AQE 处理：
  1. Shuffle 后统计每个分区的数据量
  2. 检测到倾斜分区（数据量 > 中位数 × 5 且 > 256MB）
  3. 将倾斜分区拆分为多个小子分区
  4. 分别与另一侧的对应分区 Join
  5. 最终 Union 结果
```

```scala
// 倾斜 Join 参数
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")    // 倾斜因子
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")  // 倾斜阈值
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `skewJoin.enabled` | true (3.2+) | 倾斜 Join 处理开关 |
| `skewedPartitionFactor` | 5 | 分区大小 > 中位数 × factor 才判定为倾斜 |
| `skewedPartitionThresholdInBytes` | 256MB | 分区大小超过此值才判定为倾斜 |

### 图解：AQE 倾斜 Join

```
倾斜分区（10GB）                    正常分区（100MB × 199 个）
    │                                    │
    ▼                                    ▼
拆分 128MB 子分区 × 80                 直接 Join
    │
    ▼
每个子分区分别与另一侧 Join           另一侧也按 key 分拆
    │
    ▼
Union 结果
```

倾斜分区的拆分策略：

```
原始倾斜分区 key = "北京"，数据量 10GB

拆分为多个子分区：
  "北京_0" (128MB)
  "北京_1" (128MB)
  "北京_2" (128MB)
  ...共 80 个子分区

另一侧表按 key = "北京" 过滤后，复制到所有子分区
每个子分区独立 Join → Union 结果

好处：原本 1 个 Task 处理 10GB，变成 80 个 Task 各处理 128MB
```

> **踩坑经验**：AQE 只处理 Join 场景下的数据倾斜。如果你的倾斜发生在 GroupBy 场景（比如 GROUP BY city，北京占 90% 数据），AQE 不会自动处理。GroupBy 数据倾斜需要手动使用"加盐"（salting）方案来解决。

## AQE 配置详解

```scala
// 全局开关
spark.sql.adaptive.enabled = true

// 动态合并
spark.sql.adaptive.coalescePartitions.enabled = true
spark.sql.adaptive.coalescePartitions.minPartitionNum = (defaultSparkShufflePartitions)
spark.sql.adaptive.advisoryPartitionSizeInBytes = 64MB

// 动态 Join
spark.sql.adaptive.localShuffleReader.enabled = true
spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold = 30MB

// 动态倾斜
spark.sql.adaptive.skewJoin.enabled = true
spark.sql.adaptive.skewJoin.skewedPartitionFactor = 5
spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes = 256MB
```

### AQE 配置对照表

| 配置项 | 默认值 | 建议值 | 说明 |
|--------|--------|--------|------|
| `adaptive.enabled` | true (3.2+) | true | 主开关，必须开启 |
| `adaptive.coalescePartitions.enabled` | true | true | 动态合并 |
| `adaptive.coalescePartitions.minPartitionNum` | spark.sql.shuffle.partitions | 根据集群调整 |
| `adaptive.advisoryPartitionSizeInBytes` | 64MB | 64MB-256MB | 目标分区大小 |
| `adaptive.skewJoin.enabled` | true | true | 倾斜处理 |
| `adaptive.skewJoin.skewedPartitionFactor` | 5 | 5-10 | 倾斜判断宽松度 |
| `adaptive.skewJoin.skewedPartitionThresholdInBytes` | 256MB | 256MB-1GB | 倾斜阈值 |
| `adaptive.maxShuffledHashJoinLocalMapThreshold` | 30MB | 30MB-50MB | 动态 Join 切换阈值 |

## AQE 的执行原理

### AQE 的执行流程

```
查询提交
  │
  ├─ Stage 0：文件扫描 + 过滤 + 投影
  │        │
  │        └─ Shuffle 写入（MapOutputTracker 记录分区大小）
  │
  ├─ Driver 收集 Stage 0 的统计信息
  │         │
  │         ├─ 动态合并：如果分区太小，合并相邻分区
  │         ├─ 动态 Join：如果一侧数据 < 阈值，切换 BHJ
  │         └─ 动态倾斜：如果某个分区异常大，拆分
  │
  ├─ Stage 1：基于动态调整后的计划执行
  │        │
  │        └─ 继续 Shuffle → 继续 AQE 优化
  │
  └─ ...逐 Stage 执行直到完成
```

### AQE 与 Shuffle 的关系

AQE 的核心机制依赖于 Shuffle 的物化特性：

```
每个 Shuffle 的输出都被写入 Executor 的本地磁盘
写入完成后，MapOutputTracker 知道每个分区有多少数据
这些信息被 AQE 用于：
  1. 分区合并：分区很小时合并
  2. Join 切换：分区很小时切 BHJ
  3. 倾斜检测：分区很大时拆分
```

## AQE 的局限性

```scala
// 1. 只在 Shuffle 之后生效
//    如果数据从源读取直接处理（无 Shuffle），AQE 不生效
//    → 优化需要靠 Predicate Pushdown 等传统手段

// 2. 有额外开销
//    每个 Stage 完成后需要收集统计数据
//    数据量小（几 MB）时可能没必要

// 3. 对 Streaming 不适用
//    Streaming 是微批次，每批重新优化

// 4. 不支持所有算子
//    目前主要优化 Shuffle 和 Join 相关的场景
```

| 局限性 | 原因 | 应对方法 |
|--------|------|---------|
| 只在 Shuffle 后生效 | Shuffle 后才能获取精确数据分布 | 非 Shuffle 场景靠传统优化 |
| 额外开销 | 统计信息收集 + 计划重新生成 | 小数据量时考虑关掉 |
| Streaming 不适用 | 每批次独立优化 | 无 |
| GroupBy 倾斜不处理 | AQE 只处理 Join 倾斜 | 手动加盐 |

## 面试高频考点

### Q: AQE 的三个核心优化是什么？

1. **动态合并分区**：Shuffle 后根据实际数据量合并小分区，减少 Task 数量
2. **动态 Join 策略**：运行时判断是否切换为 BroadcastHashJoin
3. **动态处理倾斜**：自动拆分倾斜分区，避免单个 Task 成为瓶颈

### Q: AQE 是如何"自适应"的？

AQE 在每个 Shuffle 阶段结束后，从 Map Output Tracker 获取实际的分区数据量统计。每个 Stage 的 Shuffle 输出被物化到磁盘之后，Driver 可以查看实际数据分布，然后决定下一个 Stage 的分区数、Join 策略和倾斜处理方式。

### Q: AQE 一定能解决数据倾斜吗？

不一定。AQE 只处理 Join 场景下的数据倾斜（通过拆分倾斜分区）。对于 GroupBy 场景的数据倾斜，AQE 没有直接处理，需要手动用加盐等方案。

### Q: 为什么 AQE 只在 Shuffle 之后优化？

Shuffle 是唯一能获得完整数据分布统计的地方。在数据源读取阶段，Spark 只知道分区数（如 HDFS 文件数）但不知道每个分区的具体数据量。Shuffle 后数据按 partitionId 重新组织，Driver 可以精确统计各分区大小。

### Q: AQE 的"动态合并"和 coalesce 有什么区别？

`coalesce` 是在 Stage 开始时基于静态信息合并分区。AQE 的动态合并是在 Shuffle 完成后，基于实际数据量做更精准的合并。而且 AQE 可以用 `advisoryPartitionSizeInBytes` 控制目标分区大小，比 `coalesce` 的固定数量更灵活。

## 小结

| 优化 | 解决的问题 | 生效条件 |
|------|-----------|---------|
| 动态合并分区 | Shuffle 分区过多但数据量小 | Shuffle 完成后 |
| 动态 Join 策略 | 静态计划选了低效的 Join 方式 | Shuffle 完成后 |
| 动态处理倾斜 | 某个 key 数据量过大 | Join 场景的 Shuffle 后 |
| 建议配置 | 全部开启（3.2+ 默认） | - |
