# HDFS 架构原理与读写流程

## HDFS 定位

HDFS（Hadoop Distributed File System）是 GFS 的开源实现，专为**大文件、流式数据访问、普通硬件**设计。

```
HDFS 不适合的场景：
❌ 低延迟数据访问（ms 级请用 HBase/Redis）
❌ 大量小文件（NameNode 内存爆炸）
❌ 多写入者、文件随机修改（HDFS 只支持 append）

HDFS 最适合的场景：
✓ 大文件（GB~TB 级）的流式读写
✓ 批处理（MapReduce/Spark）的数据源和输出
✓ "写一次、读多次"的模型
```

## 核心架构

```
                   ┌──────────────┐
                   │   Client     │
                   └──┬───────┬──┘
             元数据请求 │       │ 数据读写
              ┌────────▼──┐   │
         ┌────┤ NameNode   │   │
         │    │ (元数据)    │   │
         │    └────────────┘   │
块位置信息/心跳     ▲           │
         │         │ 心跳/块报告  │
    ┌────▼─────┐   │   ┌───────▼──────┐
    │ DataNode │   │   │  DataNode    │
    │ (数据)    │   │   │  (数据)       │
    │  ┌─┐ ┌─┐ │   │   │  ┌─┐ ┌─┐ ┌─┐ │
    │  │1│ │2│ │   │   │  │1│ │3│ │4│ │
    │  └─┘ └─┘ │   │   │  └─┘ └─┘ └─┘ │
    └──────────┘   │   └──────────────┘
                   │
              ┌────▼─────┐
              │ DataNode │
              │ (数据)    │
              │  ┌─┐ ┌─┐ │
              │  │2│ │4│ │
              │  └─┘ └─┘ │
              └──────────┘
```

### 组件职责

| 组件 | 职责 | 存储内容 |
|------|------|---------|
| **NameNode** | 命名空间管理、Block 映射、副本管理 | fsimage + edits log |
| **DataNode** | 实际数据块的读写、上报 | Block 文件（磁盘） |
| **SecondaryNameNode** | 定期合并 edits + fsimage | checkpoint 文件 |
| **Client** | 文件切分、与 NN/DN 交互 | — |

> **NameNode 内存估算**：每文件 + 每 Block ≈ 150 字节。1 亿 Block ≈ 15GB NameNode 内存。

## 元数据管理

### fsimage 和 edits log

```
NameNode 持久化两种文件：

1. fsimage（文件系统镜像）— 某一时刻文件系统的完整快照
   包含：目录结构、文件属性、Block 到文件的映射

2. edits log（编辑日志）— 自上一次 fsimage 以来的所有变更
   包含：create、delete、rename、setReplication 等操作

启动时：
fsimage ─load→ 内存
edits log ─replay→ 合并到内存
→ 得到当前完整状态

之后所有变更 ─先写edits log→ 再更新内存
```

### Checkpoint 机制

```
SecondaryNameNode 定期（通常 1 小时或 edits log > 100 万条）：
1. 从 NameNode 拉取 fsimage 和 edits log
2. 在内存中将 edits 合并到 fsimage
3. 将合并后的新 fsimage 返回给 NameNode
4. NameNode 用新 fsimage + 截断后的 edits log 继续工作

效果：edits log 不会无限膨胀，启动恢复时间可控。
```

## 读取流程

```
读取 /data.txt 的流程：
┌────────┐    1. open()      ┌──────────┐
│ Client │ ────────────────→ │ NameNode │
│        │ ←──────────────── │          │
│        │  2. Block 位置列表  │          │
│        │     (DN1, DN3)   └──────────┘
│        │
│        │  3. read()（选最近的 DN）
│        ├────────────────→ ┌──────────┐
│        │ ←──────────────── │ DN1: B1  │
│        │  4. read()       └──────────┘
│        ├────────────────→ ┌──────────┐
│        │ ←──────────────── │ DN3: B2  │
└────────┘                  └──────────┘

读取特点：
- 选择最近的副本（同一节点 > 同机架 > 跨机架）
- 一个副本失败 → 自动换下一个（对客户端透明）
- 数据校验：每读一个 Chunk 都校验 checksum
```

## 写入流程（面试必考！）

```
写入 /newfile 的流程：

Client                    NameNode               DataNode Pipeline
  │                          │                       │
  │  1. create()             │                       │
  ├─────────────────────────→│                       │
  │  2. 检查权限/路径，         │                       │
  │     创建文件记录            │                       │
  │←─────────────────────────┤                       │
  │                          │                       │
  │  3. write(packet1)       │                       │
  │  缓冲区不满，先积累         │                       │
  │                          │                       │
  │  4. 缓冲区满 →             │                       │
  │     请求 Block 1 位置      │                       │
  ├─────────────────────────→│                       │
  │  5. 返回 [DN1,DN2,DN3]   │                       │
  │←─────────────────────────┤                       │
  │                          │                       │
  │  6. 建立 Pipeline          │                       │
  │     → DN1 → DN2 → DN3    │                       │
  ├──────────────────────────┼──────────────────────→│
  │                          │                 DN1→DN2→DN3
  │  7. packet 沿 Pipeline  │                       │
  │     逐级传输 + 写入 + ACK  │                       │
  │  ← ACK                   │              ← ACK ← │
  │                          │                       │
  │  8. Block 1 写满           │                       │
  │  → 请求 Block 2 位置       │                       │
  ├─────────────────────────→│                       │
  │  ... (重复)               │                       │
  │                          │                       │
  │  9. close()               │                       │
  ├─────────────────────────→│  元数据持久化到 edits    │
  │← 完成                    │← log                  │
```

### 副本放置策略

```
Block 的三个副本（默认 replication=3）：
┌──────────────────────────────────────┐
│ 第 1 个副本：客户端所在节点            │
│  (客户端非 DN → 随机选一个)           │
│                                      │
│ 第 2 个副本：与第 1 个**不同机架**     │
│  (保证跨机架容错)                     │
│                                      │
│ 第 3 个副本：与第 2 个**同机架**       │
│  (不同的节点，减少跨机架带宽)          │
└──────────────────────────────────────┘
```

## 高可用（HA）

### 为什么需要 HA

单 NameNode = 单点故障。NameNode 挂了 → 整个 HDFS 不可用 → 集群瘫痪。

### HA 架构

```
┌─────────────────────────────────────────────┐
│                  ZooKeeper                   │
│              (主备选举 + 故障检测)              │
└────┬──────────────────────────┬─────────────┘
     │ lock                     │ lock
┌────▼──────────┐        ┌──────▼──────────┐
│ ZKFC (Active) │        │ ZKFC (Standby)  │
│ 健康检测 + 锁持有│        │ 健康检测 + 争夺锁   │
└────┬──────────┘        └──────┬──────────┘
     │                          │
┌────▼──────────┐        ┌──────▼──────────┐
│ Active NN     │        │ Standby NN      │
│ 处理读写请求   │        │ 同步状态、checkpoint│
│ 写 edits      │        │ 读 edits         │
└────┬──────────┘        └──────┬──────────┘
     │ edits write              │ edits read
     └──────────┬───────────────┘
                │
    ┌───────────▼───────────┐
    │   JournalNode 集群     │
    │  (存储 edits log)      │
    │  QJM: 过半写入即成功    │
    │  JN1    JN2    JN3    │
    └───────────────────────┘
```

**ZKFC（ZooKeeper Failover Controller）**：
- 每个 NameNode 旁有一个 ZKFC 进程
- 监控 NameNode 健康状态（通过 RPC）
- 在 ZooKeeper 中持有一个临时 znode（ephemeral lock）
- 锁超时 → 另一个 ZKFC 争抢 → 成为新 Active
- Fencing 机制防止脑裂（两个 Active 同时写）

### 联邦（Federation）

```
传统 HDFS：一个集群 = 一个 NameNode
联邦 HDFS：一个集群 = 多个 NameNode（每个管不同的目录卷）

联邦配置示例：
NameNode1 管理: /user, /tmp
NameNode2 管理: /data, /warehouse
NameNode3 管理: /ml, /streaming

所有 DataNode 向所有 NameNode 注册
客户端访问不同路径 → 路由到不同的 NameNode
```

## 面试高频考点

### Q: HDFS 为什么默认 Block 是 128MB？

1. 减少寻址开销：寻址时间 ~10ms，传输速率 ~100MB/s，128MB 传输 ~1s → 寻址开销仅 1%
2. 减少 NameNode 元数据量：Block 越大 → 元数据越少 → NameNode 内存越省
3. MapReduce 效率：一个 Map Task 通常处理一个 Block，Block 太小 → Map Task 过多

### Q: SecondaryNameNode 是 NameNode 的热备吗？

**不是！** SecondaryNameNode 只是定期做 checkpoint（合并 edits + fsimage），不能直接接管 NameNode 的工作。HA 的热备叫 **Standby NameNode**。

### Q: HDFS 如何处理小文件问题？

- **Hadoop Archive (HAR)**：将小文件打包为 .har 文件
- **SequenceFile**：Key-Value 格式的合并文件
- **定期合并任务**：每天将前一天的小文件合并为大文件
- **Spark coalesce**：处理结果合并分区避免写出大量文件
- **联邦**：更多 NameNode 分担元数据

### Q: 写入时 DataNode 挂掉怎么办？

Pipeline 中某个 DN 失败 → 客户端关闭当前 Pipeline → 用剩余的完好 DN 新建 Pipeline → 继续写入 → NameNode 知道副本数不足后安排异步复制。

## 小结

| 主题 | 关键点 |
|------|--------|
| NameNode | 元数据管家，fsimage + edits log，内存瓶颈 |
| DataNode | 数据实际存储，心跳 3s 一次，块报告 1h 一次 |
| 写入 | Pipeline 逐级传输 + ACK 确认 + 副本放置策略 |
| 读取 | 最近副本优先 + checksum 校验 + 自动容错 |
| HA | Active/Standby + JournalNode (QJM) + ZKFC (锁) |
| 联邦 | 多 NN 各自管理命名空间卷，共享 DN 集群 |
