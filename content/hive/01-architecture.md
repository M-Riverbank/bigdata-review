# Hive 架构与执行流程

## 概述

Hive 是 Hadoop 生态中的数据仓库工具，它将类 SQL 查询（HiveQL）转换为 MapReduce / Tez / Spark 作业执行。Hive **不存储数据**，数据存储在 HDFS 上，Hive 只管理元数据。

```
┌────────────┐    HiveQL     ┌──────────────┐
│   Client   │ ────────────→ │    Driver     │
│  (JDBC/CLI)│               │  (解析/编译)   │
└────────────┘               └──────┬───────┘
                                    │
                          ┌─────────▼─────────┐
                          │  MetaStore (RDBMS) │
                          │  表结构/分区/列信息  │
                          └───────────────────┘
                                    │
                          ┌─────────▼─────────┐
                          │  Execution Engine  │
                          │  MR / Tez / Spark  │
                          └─────────┬─────────┘
                                    │
                          ┌─────────▼─────────┐
                          │       HDFS         │
                          │    数据存储         │
                          └───────────────────┘
```

## 核心架构组件

### 1. Driver（驱动层）

Driver 是 Hive 的大脑，负责：

| 阶段 | 功能 |
|------|------|
| **Parser（解析器）** | 将 HiveQL 解析为 AST（抽象语法树） |
| **Semantic Analyzer（语义分析器）** | 校验表/列是否存在，类型是否匹配 |
| **Logical Plan Generator** | 生成逻辑执行计划（Operator Tree） |
| **Optimizer（优化器）** | 逻辑优化：谓词下推、列裁剪、Join 重排 |
| **Physical Plan Generator** | 转换为物理执行计划（MapReduce/Tez/Spark Task） |
| **Execution Engine** | 提交任务到 YARN，监控运行状态 |

### 2. MetaStore（元数据服务）

元数据存储在 **关系型数据库**（默认 Derby，生产用 MySQL/PostgreSQL）中。

**存储的关键元数据**：
- 数据库、表、分区的基本信息
- 列名、数据类型
- 表存储格式（ORC / Parquet / Text）
- 表位置（HDFS 路径）
- 分区信息

```sql
-- MetaStore 中的关键表（MySQL 后端）
-- TBLS: 表信息
-- SDS: 存储描述（Storage Descriptor）
-- PARTITIONS: 分区信息
-- COLUMNS_V2: 列信息
-- TABLE_PARAMS / PARTITION_PARAMS: 表/分区参数
```

### 3. 执行引擎

| 引擎 | 特色 |
|------|------|
| **MapReduce** | 最早支持，稳定但慢 |
| **Tez** | DAG 执行，避免中间落盘，性能大幅提升 |
| **Spark** | 内存计算，适合迭代型查询 |

## HiveQL 执行流程详解

以 `SELECT a.name, SUM(b.amount) FROM users a JOIN orders b ON a.id=b.uid GROUP BY a.name` 为例：

```
Step 1: Parser → AST
        SELECT
        ┌────┴────┐
      JOIN      GROUP BY
     ┌──┴──┐     SUM
   users orders

Step 2: Semantic Analyzer
        → 检查 users/orders 表是否存在
        → 检查 name, amount, id, uid 列是否存在
        → 检查 JOIN 条件类型是否兼容

Step 3: Logical Plan (Operator Tree)
        TableScan(users) ─┐
                           ├→ JoinOperator → GroupByOperator → SelectOperator
        TableScan(orders)─┘

Step 4: Optimizer
        应用规则：
        - Predicate Pushdown（谓词下推）
        - Column Pruning（列裁剪）
        - Map Join（小表广播）
        - Group By 优化

Step 5: Physical Plan → MapReduce/Tez Job

Step 6: 提交到 YARN 执行
```

## Hive 数据模型

```
Hive 数据仓库分层结构：

┌─────────────────────────────────┐
│         Database (数据库)        │
│  ┌───────────────────────────┐  │
│  │     Table (表)             │  │
│  │  ┌─────────────────────┐  │  │
│  │  │   Partition (分区)   │  │  │
│  │  │  date=2024-01-01    │  │  │
│  │  │  ┌──────────────┐   │  │  │
│  │  │  │ Bucket (分桶) │   │  │  │
│  │  │  │ hash(user_id) │   │  │  │
│  │  │  │ % 10          │   │  │  │
│  │  │  └──────────────┘   │  │  │
│  │  └─────────────────────┘  │  │
│  └───────────────────────────┘  │
└─────────────────────────────────┘
```

### 分区 vs 分桶

| 维度 | 分区 (Partition) | 分桶 (Bucket) |
|------|-----------------|---------------|
| 原理 | 按字段值创建子目录 | 按字段 Hash 值分散 |
| 数量 | 可能很大（上万） | 通常固定（如 256） |
| 粒度控制 | 用户显式指定 | 系统自动分配 |
| 适用场景 | 按日期/地域查询 | JOIN/Sampling |
| HDFS 布局 | `/table/dt=2024-01-01/` | `/table/000000_0` |

```sql
-- 创建分区表
CREATE TABLE sales (
  user_id INT,
  amount DECIMAL(10,2)
) PARTITIONED BY (dt STRING, region STRING)
STORED AS ORC;

-- 创建分桶表
CREATE TABLE user_info (
  user_id INT,
  name STRING
) CLUSTERED BY (user_id) INTO 64 BUCKETS
STORED AS ORC;
```

## Hive 优化机制

### 1. 谓词下推（Predicate Pushdown）

将过滤条件尽可能**推到数据读取层**，减少扫描数据量。

```sql
-- Hive 自动将 WHERE 条件下推到 TableScan 阶段
SELECT * FROM orders WHERE dt = '2024-01-01' AND amount > 100;
-- → 只扫描 dt=2024-01-01 的分区，读取时过滤 amount > 100
```

### 2. Map Join（大表 JOIN 小表）

小表数据广播到所有 Map 节点，**在 Map 端完成 JOIN**，避免 Shuffle。

```sql
-- 显式指定 Map Join
SELECT /*+ MAPJOIN(small_table) */
  a.*, b.name
FROM large_table a
JOIN small_table b ON a.id = b.id;
```

自动 Map Join 触发条件：`hive.auto.convert.join=true`，小表大小阈值：`hive.mapjoin.smalltable.filesize`（默认 25MB）

### 3. 列裁剪（Column Pruning）

只读取 SQL 中实际引用的列，对 ORC/Parquet 列式存储效果显著。

### 4. Group By 优化

```sql
-- Map 端预聚合（Combiner），减少 Shuffle 数据量
SET hive.map.aggr=true;

-- 数据倾斜优化
SET hive.groupby.skewindata=true;
```

## 文件存储格式对比

| 格式 | 压缩 | 列式 | 查询速度 | 使用场景 |
|------|------|------|---------|---------|
| TextFile | 无 | 否 | 慢 | 数据交换 |
| SequenceFile | 支持 | 否 | 中等 | 中间数据 |
| ORC | 高 | 是 | 快 | Hive 主力格式 |
| Parquet | 高 | 是 | 快 | Spark/Hive 通用 |

> **面试建议**：被问到存储格式时，一定要强调 ORC 的谓词下推 + 列裁剪优势。

## 面试高频考点

### Q: Hive 和传统 RDBMS 的区别？

| 维度 | Hive | RDBMS |
|------|------|-------|
| 数据规模 | PB 级 | TB 级 |
| 查询延迟 | 秒~分钟（离线） | 毫秒~秒（OLTP） |
| 索引 | 有限支持 | 完善的 B+ Tree |
| 更新/删除 | 有限支持（ACID 表） | 完整 CRUD |
| 执行引擎 | MR/Tez/Spark | 单机执行器 |

### Q: Hive MetaStore 的作用？挂了有什么影响？

MetaStore 存储所有表/分区/列的元数据。如果 MetaStore 挂了，**无法执行任何 DDL/DML 操作**（查不了已有的表），但已有的 HDFS 数据文件不会丢失。生产环境一般部署 Metastore HA（多实例 + 统一 MySQL 后端）。

### Q: ORC vs Parquet 怎么选？

- ORC 和 Hive 绑定更深，压缩比更高，对谓词下推支持更完善
- Parquet 生态更通用（Spark/Flink/Impala 都原生支持），类型系统更丰富
- 纯 Hive 环境选 ORC，多引擎环境选 Parquet

## 小结

| 核心组件 | 关键点 |
|---------|--------|
| Driver | SQL → AST → 逻辑计划 → 优化 → 物理计划 → 执行 |
| MetaStore | 元数据存 MySQL，影响所有 DDL/DML |
| 分区/分桶 | 分区=按值建目录，分桶=按 Hash 分散 |
| 优化策略 | 谓词下推、列裁剪、Map Join、Map 端预聚合 |
