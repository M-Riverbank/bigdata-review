# YARN 资源调度框架

## YARN 的诞生

Hadoop 1.x 的 JobTracker 既管资源又管调度 → 单点瓶颈 + 仅支持 MR。YARN 将两大职责分离。

```
Hadoop 1.x (MRv1):                    Hadoop 2.x+ (YARN):
┌──────────────┐                      ┌──────────────┐
│  JobTracker  │ ← 资源+调度耦合        │ResourceManager│ ← 只管资源
└────┬────┬────┘                      └──────┬───────┘
     │    │                                  │
┌────▼┐ ┌─▼───┐                    ┌─────────▼──────────┐
│ TT  │ │ TT  │                    │ ApplicationMaster  │ ← 管单个应用调度
└────┘ └─────┘                    └─────────┬──────────┘
         只支持 MR                           │
                                    ┌───────▼───────┐
                                    │ NodeManager   │ ← 节点资源
                                    └───────────────┘
                                    MR / Spark / Flink / Tez 都支持
```

## 核心架构

```
┌──────────────────────────────────────────────────────────┐
│                    ResourceManager (RM)                   │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────────┐  │
│  │ Scheduler    │  │Applications  │  │ SchedulerLoader│  │
│  │ (资源分配)     │  │ Manager (ASM)│  │ (加载调度器)    │  │
│  └──────────────┘  └──────────────┘  └───────────────┘  │
└─────────────────────┬────────────────────────────────────┘
                      │
         ┌────────────┼────────────┐
         │            │            │
┌────────▼───┐  ┌─────▼──────┐  ┌─▼──────────┐
│ NodeManager│  │ NodeManager│  │ NodeManager │
│ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐  │
│ │Contain.│ │  │ │Contain.│ │  │ │Contain.│  │
│ │(Spark  │ │  │ │(Flink  │ │  │ │(Tez    │  │
│ │ Executor│ │  │ │ TM)   │ │  │ │ Task) │  │
│ └────────┘ │  │ └────────┘ │  │ └────────┘  │
│ ┌────────┐ │  │ ┌────────┐ │  │            │
│ │AppMaster│ │  │ │Contain.│ │  │            │
│ │(app)   │ │  │ │(MR map)│ │  │            │
│ └────────┘ │  │ └────────┘ │  │            │
└────────────┘  └────────────┘  └────────────┘
```

### 核心组件

| 组件 | 职责 |
|------|------|
| **ResourceManager** | 集群资源总管：调度、分配 Container、管理 NM 心跳 |
| **NodeManager** | 单节点资源管家：上报资源、启停 Container、监控资源使用 |
| **ApplicationMaster** | 单应用管家：申请 Container、启停 Task、处理失败 |
| **Container** | 资源抽象：封装 CPU + 内存，Task 在其中运行 |

## 调度器对比

### Capacity Scheduler（推荐生产用）

```
┌────────────────────────────┐
│        root (100%)         │
├──────────┬────────┬────────┤
│  ETL     │  Ad-hoc│  ML    │
│  40%     │  30%   │  30%   │
│  ┌────┐  │  ┌────┐│  ┌────┐│
│  │FIFO│  │  │FIFO││  │FIFO││
│  └────┘  │  └────┘│  └────┘│
└──────────┴────────┴────────┘

特点：
- 每队列保证最小资源 (capacity)
- 空闲时可弹性借用
- 队列内默认 FIFO
- 收回借用资源时优雅终止（等待 task 完成）
```

```xml
<!-- capacity-scheduler.xml -->
<property>
  <name>yarn.scheduler.capacity.root.queues</name>
  <value>etl,adhoc,ml</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.etl.capacity</name>
  <value>40</value>
</property>
<property>
  <name>yarn.scheduler.capacity.root.etl.maximum-capacity</name>
  <value>80</value>
</property>
```

### Fair Scheduler（推荐研究/开发用）

```
资源动态分配过程：
T0: 只有 Job A → 100%
T1: Job B 提交 → A:50%, B:50%
T2: Job C 提交 → A:33%, B:33%, C:33%
T3: Job A 完成 → B:50%, C:50%

特点：
- 按权重（weight）动态分配
- 新应用提交即刻获得公平份额
- 支持多种策略（FIFO/FAIR/DRF）
```

```xml
<!-- fair-scheduler.xml -->
<queue name="research">
  <minResources>0 mb, 0 vcores</minResources>
  <maxResources>200000 mb, 100 vcores</maxResources>
  <schedulingPolicy>fair</schedulingPolicy>
  <weight>2.0</weight>  <!-- 权重是其他队列的两倍 -->
</queue>
```

## 资源模型

### Container

```
Container = { memory, vCores, [GPU], [Disk] }

请求：AM 向 RM 请求 "2GB 内存 + 1 vCore"
分配：RM 找到满足条件的 NM，在该 NM 上创建 Container
隔离：Linux Cgroups（CPU 限制）+ 内存限额
```

### 内存与 CPU 配置

```xml
<!-- yarn-site.xml -->

<!-- NM 可分配给 Container 的总内存（建议物理内存的 75-80%） -->
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>65536</value>  <!-- 64GB -->
</property>

<!-- NM 可用的总 vCores -->
<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>16</value>
</property>

<!-- Container 的最小/大内存 -->
<property>
  <name>yarn.scheduler.minimum-allocation-mb</name>
  <value>1024</value>  <!-- 1GB，避免资源碎片 -->
</property>
<property>
  <name>yarn.scheduler.maximum-allocation-mb</name>
  <value>32768</value>  <!-- 32GB，防止单 Container 占用整机 -->
</property>
```

**内存调优公式**：
```
总内存 = OS 系统预留 + YARN NM 内存
YARN NM 内存 = 所有 Container 内存之和
Container 内存 = Spark Executor 堆内 + 堆外 (spark.yarn.executor.memoryOverhead)
```

## 应用运行流程

```
spark-submit --master yarn --deploy-mode cluster

1. Client → RM: 提交应用 + 上传资源
   └→ RM 返回 Application ID

2. RM → NM: 分配第一个 Container 启动 ApplicationMaster
   └→ SparkAppMaster 启动

3. AM → RM: 注册自己

4. AM → RM: 申请 Executor Container
   └→ "我要 10 个 Container，每个 4GB + 2 cores"

5. RM → AM: 分配 Container（根据调度策略）
   └→ 返回 [Container1@NM1, Container2@NM2, ...]

6. AM → NM: 在分配的 Container 中启动 Executor
   └→ CoarseGrainedExecutorBackend

7. Executor → Driver: 注册（Driver 在 AM 中，Cluster 模式）

8. Driver → Executor: 分发 Task、DAGScheduler 调度

9. 所有 Job 完成 → AM 向 RM 注销自己
```

## 面试高频考点

### Q: Capacity Scheduler vs Fair Scheduler 怎么选？

| 维度 | Capacity | Fair |
|------|----------|------|
| 资源保障 | 最小容量保证 + 弹性 | 按权重动态分配 |
| 队列内部 | 默认 FIFO | 可 FIFO/FAIR/DRF |
| 抢占 | 资源收回时优雅终止 | 可抢占 |
| 适用 | 多租户生产（SLA 保障） | 开发/研究环境 |
| 单应用独占 | 有上限（maximum-capacity） | 有上限（maxResources） |

### Q: Container 分配为什么不能跨 NodeManager？

Container 必须在单个 NM 的资源范围内——如果 NM 剩余内存 5GB，不能分配一个 6GB 的 Container。这就是为什么要避免资源碎片：残留的 1GB-2GB 小空间无法分配大 Container。

### Q: NodeManager 心跳 10 分钟没收到怎么办？

RM 认为该 NM 死亡 → 该 NM 上的所有 Container 标记为失败 → AM 重新申请 Container → 应用重试 Task。

### Q: Spark Executor 的 overhead 是什么？

`spark.yarn.executor.memoryOverhead`（默认 executor.memory × 0.1，最少 384MB）是 Executor 堆外内存（用于 Java 原生内存、线程栈、I/O 缓冲区等）。如果 overhead 不够 → Executor 可能被 YARN 的 Cgroups 杀掉。

## 小结

| 概念 | 核心 |
|------|------|
| RM | 全局资源总管 + 调度器 |
| NM | 节点 Agent + Container 启停 |
| AM | 单应用管家 + 资源申请 |
| Container | 资源的逻辑封装 |
| Capacity | 队列最小保障 + 弹性（生产首选） |
| Fair | 按权重动态均分（公平性优先） |
