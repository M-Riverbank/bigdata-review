# HBase RowKey 设计原则

## 为什么 RowKey 设计至关重要

HBase 是一个**按 RowKey 字典序排序的 KV 存储**，所有数据按 RowKey 排序后分布在各个 Region 中。RowKey 设计直接决定：

- **数据热点**：读写是否均匀分布在集群各节点
- **扫描效率**：是否能用前缀扫描满足业务查询
- **存储空间**：RowKey 长度影响每行索引开销

```
HBase 数据分布示意：

Region 0: [0x00 — 0x33)     →  RegionServer A
Region 1: [0x33 — 0x66)     →  RegionServer B
Region 2: [0x66 — 0x99)     →  RegionServer C
Region 3: [0x99 — 0xFF)     →  RegionServer A

如果 RowKey 是连续递增的时间戳 →
所有写入都落在最后一个 Region → 热点问题！
```

## 核心设计原则

### 原则 1：长度原则

**RowKey 越短越好**，原因：

- 每行数据都存储 RowKey 的副本
- HFile 中 RowKey 不被压缩（KeyValue 的 Key 部分）
- MemStore 中每行也存 RowKey
- 建议长度：**16-64 字节**

```
❌ 差: "20240115_user_13800138000_purchase_order_daily"
✓ 好: "100001#13800138000#purchase"  (压缩为短 ID)
```

### 原则 2：散列原则 — 避免热点

**核心思想**：让 RowKey 的前缀尽可能分散，使数据均匀分布到各个 Region。

| 方法 | 示例 | 优点 | 缺点 |
|------|------|------|------|
| **加盐（Salting）** | `hash(user_id) % 100 + user_id` | 绝对均匀 | 查单用户需扫100次 |
| **Hash 前缀** | `md5(user_id)[0:4] + user_id` | 均匀且可查 | 前缀无业务含义 |
| **反转 Key** | `reverse(timestamp)` | 新数据均匀分布 | 时间范围扫描不便 |
| **随机前缀** | `rand(100) + key` | 简单 | 查询需遍历所有分片 |

```java
// 加盐方式的 RowKey 设计
// 需求：用户行为表，经常按 user_id 查询
String rowKey = Math.abs(userId.hashCode()) % 100 + "_" + userId + "_" + timestamp;
// → "42_user_1001_20240115120000"
```

### 原则 3：业务含义在前

**将查询最频繁的维度放在 RowKey 前面**，利用 HBase 的**前缀扫描**。

```
业务需求：查询某用户的最近N条订单
RowKey设计：user_id + (Long.MAX_VALUE - timestamp)
           → "1001_9223370404895807999"

优点：userId 作为前缀，scan '1001_' 即可获得该用户全部订单
而且按时间倒序排列！
```

```java
// 实际应用：用户订单表
// 需要查：某用户的所有订单 + 按时间倒序
byte[] rowKey = Bytes.add(
    Bytes.toBytes(userId),           // 业务前缀
    Bytes.toBytes(Long.MAX_VALUE - orderTime)  // 反转时间戳（倒序）
);
```

### 原则 4：避免单调递增

**时间戳连续递增 → 所有写入到同一个 Region → 单节点热点**

```java
// ❌ 不要这样：时间戳开头的 RowKey
byte[] bad = Bytes.add(Bytes.toBytes(System.currentTimeMillis()), userIdBytes);

// ✓ 解决方案一：时间戳反转
byte[] good1 = Bytes.add(
    Bytes.toBytes(Long.MAX_VALUE - System.currentTimeMillis()),
    userIdBytes
);

// ✓ 解决方案二：Hash 前缀 + 时间
byte[] good2 = Bytes.add(
    Bytes.toBytes(MurmurHash.hash(userIdBytes) % 1000),
    Bytes.toBytes(System.currentTimeMillis()),
    userIdBytes
);

// ✓ 解决方案三：时间取模
byte[] good3 = Bytes.add(
    Bytes.toBytes(System.currentTimeMillis() % 1000),
    Bytes.toBytes(System.currentTimeMillis()),
    userIdBytes
);
```

## 经典设计模式

### 模式 1：多维度查询 — 冗余设计

如果既要按时间查，又要按用户查，可能无法用一个 RowKey 满足。解决方案：

1. **创建多张表**，不同 RowKey（类似关系库的索引）
2. **使用二级索引**（Phoenix / ElasticSearch）

```
表1：按用户查询
RowKey: userId + timestamp → [订单数据]

表2：按日期查询
RowKey: date + userId → [订单数据]
```

### 模式 2：预分区（Pre-splitting）

在建表时指定 Split Keys，提前划分 Region，避免自动分裂。

```bash
# HBase Shell 创建预分区表
create 'user_events',
  {NAME => 'cf', VERSIONS => 3},
  SPLITS => ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']
```

```java
// Java API 使用 HexSplit
byte[][] splits = new byte[255][];
for (int i = 0; i < 255; i++) {
  splits[i] = new byte[]{(byte) (i + 1)};
}
admin.createTable(tableDescriptor, splits);
```

### 模式 3：RowKey 的字段分隔符

使用固定长度或分隔符来区分 RowKey 中的不同字段：

```
// 固定长度（推荐，便于按位切分）
RowKey: [8B_userId][8B_reverseTimestamp][4B_eventType]
     → 无需分隔符，按偏移量解析

// 分隔符（灵活，但占额外空间）
RowKey: userId + "#" + reverseTimestamp + "#" + eventType
```

## Region 热点诊断

```bash
# 检查是否出现热点
hbase> status 'simple'
# 查看各 RegionServer 的请求量

# 查看某个表的 Region 分布
hbase> list_regions 'table_name'
```

图形化工具：
- HBase Master Web UI (端口 16010)
- Ganglia / Grafana 监控 RegionServer QPS

## 常见错误案例

### ❌ 错误 1：直接用时间戳做 RowKey 前缀

```java
// 所有新数据都写入最后一个 Region
byte[] rowKey = Bytes.toBytes(String.valueOf(System.currentTimeMillis()));
```

### ❌ 错误 2：RowKey 过长

```
// 每个 RowKey 100+ 字节，百万行就 100MB 的纯 RowKey 开销
"2024-01-15_user_13800138000_action_purchase_order_from_app"
```

### ❌ 错误 3：没有考虑查询模式

```java
// 只能按 RowKey 精确查询，无法按时间范围扫描
// 如果想查"最近7天"的数据 → 全表扫描！
byte[] rowKey = Bytes.toBytes(userId + "_" + random.nextInt(100));
```

## 面试高频考点

### Q: HBase RowKey 设计原则？

**四句话回答**：
1. **长度短**：建议 16-64 字节，减少存储开销
2. **散列好**：避免单调递增导致热点，常用加盐/Hash/反转
3. **业务前缀**：高频查询维度放前面，利用前缀扫描
4. **考虑预分区**：建表时合理划分 Region

### Q: 如何解决热点写入问题？

- 加盐（Salting）：`hash(user_id) % N + user_id`
- Hash 前缀：`md5(user_id)[:4] + user_id`
- 反转时间戳：`Long.MAX_VALUE - ts`
- 预分区：提前按 RowKey 范围划分 Region

### Q: 多维度查询怎么处理？

- 冗余写多张表（空间换时间）
- 使用 Phoenix SQL 层（支持二级索引）
- ES + HBase 组合（ES 做索引，HBase 做存储）
- 设计合适的 RowKey 复合前缀

## 小结

| 设计要素 | 指导原则 |
|---------|---------|
| 长度 | 16-64 字节，越短越好 |
| 散列 | 避免热点，前缀需均匀分布于各 Region |
| 业务优先 | 最常用的查询维度作为 RowKey 前缀 |
| 时间处理 | 禁止单调递增的时间戳做前缀 |
| 预分区 | 建表时定好 Split Keys |
| 分隔符 | 优先用固定长度字段，非必要不用分隔符 |
