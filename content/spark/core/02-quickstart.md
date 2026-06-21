# Spark Core — 快速开始与开发环境搭建

## Scala 还是 Python？

很多初学者面临的第一个灵魂拷问就是："Spark 到底用 Scala 还是 Python？"

其实这个问题的答案取决于你的**岗位方向**和**职业目标**。Spark 虽然提供了多种语言的 API，但在面试和实际工作中，不同语言对应的场景差异很大。我们来逐一拆解。

### 为什么会有多种语言选择？

Spark 底层是用 Scala（JVM 语言）编写的，所以 Scala 是 Spark 的"母语"——这意味着 Scala API 总是最新的，功能的覆盖最全。Python 版本的 PySpark 本质上是 Scala API 的封装，通过 `Py4J` 网关调用 JVM 上的 Spark 核心逻辑。SQL 则是最上层的一层抽象，适合分析师快速取数。

> **面试点**：面试官如果问 "PySpark 和 Scala Spark 性能有区别吗？"，答案是：**对于 DataFrame/SQL API，性能几乎没有差别**，因为执行引擎都是 Catalyst + Tungsten 全在 JVM 侧。区别在于 UDF 的序列化开销——PySpark UDF 需要序列化到 Python 进程执行，而 Scala UDF 直接在 JVM 内运行。

### 三种语言的对比

| 语言 | 适用场景 | 面试占比 | 学习曲线 | 开发效率 |
|------|---------|---------|---------|---------|
| **Scala** | 核心开发、性能调优、Spark 源码级问题 | 60%（大数据开发岗） | 较陡（函数式 + 类型系统） | 中 |
| **PySpark** | 数据分析和 ML 快速原型 | 30%（数仓/分析岗） | 平缓（Python 上手快） | 高 |
| **SQL** | 即席查询、报表 | 10% | 最低（会 SQL 就会） | 极高（但灵活性有限） |

### 在实际项目中的应用

- **Scala 阵营**：如果你在一家互联网公司的**大数据基础架构组**，需要自己开发 Spark 作业调度框架、自定义数据源连接器、或修改 Spark 源码做性能优化——那你逃不掉 Scala。
- **PySpark 阵营**：如果你是**数据工程师**或**数据分析师**，主要工作是写 ETL 任务、跑数据报表、训练模型——PySpark 是你的好朋友。
- **SQL 阵营**：很多公司的**即席查询平台**直接提供 Spark SQL 接口，分析师会用 SQL 就够了。

> **面试建议**：面试大数据开发岗时优先掌握 Scala API，面试官期待看到你理解 RDD/DataFrame 算子的类型签名和函数式特性。小白的一个常见误区是 "会 PySpark 就够了"——但在面试中，Scala 代码片段出现的概率远高于 Python。

### 踩坑经验

> **实际踩坑**：在同一个项目中混合使用 Scala 和 PySpark 可能会导致依赖冲突。比如 Scala 作业依赖了某个版本的 `protobuf` JAR，而 PySpark 作业在同一个 YARN 队列中运行时，不同的 classpath 可能会导致不可预知的 `NoSuchMethodError`。推荐的做法是：一个项目统一用一种语言。

## 环境搭建

环境搭建这一块看起来很基础，但恰恰是面试中的 "送分题" 也是 "送命题"——很多候选人能在白板上写出复杂的算子优化，却在被问到 "Spark 本地环境怎么搭" 时卡住。

### 前置条件：你真的需要 Hadoop 吗？

很多初学者以为 Spark 必须依赖 Hadoop，其实这是一个常见误区。Spark 本身是一个**独立的计算引擎**，它的运行并不需要 Hadoop 的 HDFS 或 YARN。你可以用 `local` 模式在单机上跑，也可以用 `standalone` 集群管理器，甚至可以直接跑在 Kubernetes 上。

> **面试点**：Spark 的 "无 Hadoop 版本"（`-without-hadoop` 后缀）只包含了 Spark 自身，不捆绑 Hadoop 客户端 JAR。如果你需要从 HDFS 读数据，你仍然需要一个 `HADOOP_HOME` 或把 hadoop-client JAR 放到 classpath 中。直接下载 `spark-x.x.x-bin-hadoop3.tgz`（带 Hadoop 版本）才是大多数开发者的选择。

### 本地开发模式

Linux/Mac 下的环境搭建最方便。Windows 用户需要额外安装 `winutils.exe`，这是一个常见的坑。

```bash
# 1. 安装 Java 8/11/17（推荐 JDK 17，性能和 GC 最优）
java -version  # 确认版本

# 2. 下载 Spark（推荐带 Hadoop 的版本，省心）
wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz
tar -xzf spark-3.5.1-bin-hadoop3.tgz
cd spark-3.5.1-bin-hadoop3/

# 3. 配置环境变量（建议写入 ~/.bashrc 或 ~/.zshrc）
export SPARK_HOME=$(pwd)
export PATH=$PATH:$SPARK_HOME/bin

# 4. 验证安装
spark-shell --version
```

### Windows 用户的特别提醒

> **踩坑经验**：Windows 上运行 Spark 需要额外处理 Hadoop 本地库。如果你在 Windows 上启动 `spark-shell` 报错 `Failed to locate the winutils binary`，解决方案是：
> 1. 从 GitHub 下载对应 Hadoop 版本的 `winutils.exe`
> 2. 放到一个目录（如 `C:\hadoop\bin\`）
> 3. 设置环境变量 `HADOOP_HOME=C:\hadoop` 并加入 PATH
> 
> 或者更省事的方案：直接使用 Docker 镜像（`docker run -it --rm apache/spark:3.5.1 ./bin/spark-shell`），无需配置任何本地环境。

### Spark Shell — 交互式探索

Spark Shell 是学习和调试 Spark 的**最强利器**。它提供了一个 REPL（Read-Eval-Print Loop）环境，加载一行代码立即看到结果，省去了 "写代码 → 编译 → 打包 → 提交" 的冗长流程。

```bash
# 启动 Scala shell（自动创建 SparkSession）
spark-shell --master local[*]

# 启动 PySpark shell
pyspark --master local[*]
```

`local[*]` 的含义是：在本地运行，使用本机所有可用的 CPU 核心数。`*` 可以替换成具体数字，如 `local[4]` 表示用 4 个核。

```scala
// Spark Shell 中自动创建的变量
sc      // SparkContext（旧 API 入口，1.x 时代的主要入口）
spark   // SparkSession（新 API 入口，2.0+ 统一入口）

// 快速验证 — 分布式 range 创建百万级数据集
val df = spark.range(1000000)
df.count()  // 1000000 — 验证集群能正确执行

// 你还可以立即查看执行计划
df.explain()
// == Physical Plan ==
// *Range (1) rows=1000000
```

> **面试点**：`spark-shell` 启动时自动创建的 `sc` 和 `spark` 意味着你不必手动 `val spark = SparkSession.builder()...`。但在 IDE 中写应用时，这个步骤是必须的。面试中经常问："Spark Shell 和 IDE 中写应用有什么区别？"——答案就是入口对象的创建方式和资源的自动管理。

### 日志级别的调整

Spark Shell 默认会输出大量 INFO 日志，影响阅读。启动后可以先执行：

```scala
import org.apache.log4j.{Level, Logger}
Logger.getLogger("org").setLevel(Level.WARN)
Logger.getLogger("akka").setLevel(Level.WARN)
```

或者启动时指定配置文件 `log4j.properties`，将 root logger 级别设为 `WARN`。

### IDE 项目配置（SBT）

当你的需求超出 REPL 交互式验证的范畴，需要构建一个可重复运行的 Spark 应用时，就需要用 IDE（IntelliJ IDEA 是首选）配一个构建工具了。

```scala
// build.sbt
name := "spark-demo"
version := "1.0"
scalaVersion := "2.13.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.1",
  "org.apache.spark" %% "spark-sql" % "3.5.1"
)
```

关键点说明：

- `%%` 的含义：SBT 的 `%%` 会根据你设置的 `scalaVersion` 自动附加 Scala 版本后缀。比如 `scalaVersion := "2.13.10"` 时，`"org.apache.spark" %% "spark-core"` 实际解析为 `"org.apache.spark" % "spark-core_2.13"`。
- **依赖作用域**：在开发环境用 `compile` 作用域（默认），在打包时如果你是构建一个 "胖 JAR" 提交到已有 Spark 环境的集群，务必把所有 Spark 依赖的 scope 改成 `provided`，否则会导致 JAR 冲突。

> **踩坑经验**：曾有一个真实案例——开发者在 `build.sbt` 中没有设置 Spark 依赖为 `provided`，打出了包含 `spark-core` 和 `spark-sql` 全部 class 的胖 JAR（约 200MB）。提交到集群后 Job 一直报 `NoSuchMethodError`，排查了半天发现是 JAR 中的本地 Spark 版本和集群上的 Spark 版本有细微差异，classloader 加载了错误的版本。修复方式就是把 Spark 依赖改为 `provided`。

### Maven 配置方案

如果你团队统一用 Maven，配置也是类似的：

```xml
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-core_2.13</artifactId>
  <version>3.5.1</version>
  <scope>provided</scope>
</dependency>
<dependency>
  <groupId>org.apache.spark</groupId>
  <artifactId>spark-sql_2.13</artifactId>
  <version>3.5.1</version>
  <scope>provided</scope>
</dependency>
```

## Spark 应用提交

写完代码后，怎么让它在集群上跑起来？这是面试中必问的环节之一。

### spark-submit 命令

`spark-submit` 是 Spark 提供的一个**统一脚本**，它屏蔽了底层集群管理器的差异——无论你的集群是 YARN、Kubernetes 还是 Spark Standalone，都是用同一个 `spark-submit` 命令提交应用。

```bash
# 一个典型的生产提交命令
spark-submit \
  --class com.example.MyApp \
  --master yarn \
  --deploy-mode cluster \
  --executor-memory 4g \
  --num-executors 10 \
  --executor-cores 4 \
  --queue default \
  my-app.jar \
  arg1 arg2
```

### 参数详解

下面这张表是面试中最高频出现的参数集，建议牢牢记住：

| 参数 | 说明 | 建议值 | 面试常考 |
|------|------|--------|---------|
| `--master` | 集群管理器类型 | yarn / k8s:// / spark:// | 不同 manager 的优缺点 |
| `--deploy-mode` | Driver 运行位置 | client（调试）/ cluster（生产） | 两者的本质区别 |
| `--executor-memory` | 每个 Executor 的 JVM 堆内存 | 4g~32g（视数据量） | GC 调优如何配合 |
| `--num-executors` | Executor 数量 | 根据队列资源定 | 如何估算 |
| `--executor-cores` | 每个 Executor 的 CPU 核数 | 2~5（建议 4） | 核数太多会怎样 |
| `--driver-memory` | Driver 进程内存 | 2g~8g | 什么操作会撑爆 Driver |

> **面试点**：面试中常问的一个经典问题："`deploy-mode cluster` 和 `client` 有什么区别？" 简单回答就是 Driver 的位置不同。`client` 模式下 Driver 运行在提交作业的那台机器上（比如你的开发机），所有 `println` 和日志都输出在本地终端，方便调试。`cluster` 模式下 Driver 在集群的某个 Worker 节点上运行，你就看不到 stdout 日志了，需要通过 `yarn logs -applicationId <appId>` 来查看。

### 资源估算方法

面试中还可能问到："给你一个 1TB 的数据集，集群有 20 台机器每台 64GB 内存 16 核，你怎么设置这些参数？"

大致思路：
1. **数据保留**：1TB 原始数据，考虑序列化和副本，实际占用 ~1.2TB
2. **可用内存**：每台留 8GB 给 OS 和系统进程，可用 56GB/台
3. **Executor 规划**：每台 3 个 Executor，每个 Executor 约 18GB 内存（56/3 ≈ 18）
4. **核数**：每个 Executor 给 4 核，3 * 4 = 12 核，留 4 核给 OS
5. **总 Executor 数**：20 * 3 = 60 个
6. **shuffle 分区**：建议设为 executor 总核数的 2-3 倍 ≈ 60*4*2 = 480

> **踩坑经验**：不要给每个 Executor 分配太多核心（比如 8 个以上），因为 HDFS 的写入吞吐量和并发 task 数量并不成正比——当 Executor 内并发的 task 超过 5 个时，I/O 和 GC 的竞争会导致收益递减。一个普遍的 "黄金比例" 是每个 Executor 4-5 核、16-32GB 内存。

### SparkSession 初始化

从 Spark 2.0 开始，`SparkSession` 是 Spark 的**统一入口**。它整合了原来 Spark 1.x 时代的 `SparkContext`、`SQLContext`、`HiveContext` 等多个入口点，变得更加简洁。

```scala
import org.apache.spark.sql.SparkSession

// 2.0+ 统一入口：一个 Session 搞定所有事情
val spark = SparkSession.builder()
  .appName("MyApp")                            // 应用名称，在 YARN UI 上看到的名字
  .config("spark.sql.shuffle.partitions", "200") // 调整 shuffle 分区数（默认 200）
  .config("spark.executor.memory", "4g")        // 也可在代码中指定资源参数
  .enableHiveSupport()                          // 如果需要 Hive 集成（元数据 + Hive SQL）
  .getOrCreate()                                // 获取已有 Session 或创建新的

// 旧版 SparkContext 可以通过 spark.sparkContext 访问
val sc = spark.sparkContext
```

关键设计理念：
- **`getOrCreate()` 而非 `new SparkSession()`**：在多 session 场景下（如 Notebook），如果已经存在同名的 Session，直接复用，避免重复创建。
- **配置优先级**：代码中 `config()` 的设置 > `spark-submit` 传递的参数 > `spark-defaults.conf` 中的配置 > Spark 默认值。你可以在代码中硬编码某些不能由用户覆盖的参数，也可以在 `spark-submit` 中覆盖代码里的默认值。

> **面试点**：`spark.sql.shuffle.partitions` 默认是 200，但在实际生产中这个值通常需要根据数据量调整。数据量小时设 200 会导致大量 "空 Task" 浪费资源；数据量大时 200 又可能不够，每个 Task 处理的数据过多导致 OOM。常用的设置公式是：`shuffle partitions = (总数据量 / 128MB)` 或者 `shuffle partitions = (总核心数 * 2)`，取较大值。

## 第一个 Spark 应用

学任何编程语言或框架的 "Hello World" 都是 WordCount。Spark 的 WordCount 虽然简单，但已经包含了 Spark 最核心的数据处理范式。

### 使用 DataFrame API 的 WordCount

```scala
// WordCount — 大数据的 Hello World
import spark.implicits._  // 导入隐式转换，提供 $ 符号等 DSL 语法

val textFile = spark.read.textFile("hdfs:///data/input.txt")
// textFile: Dataset[String] — 每一行是一个 String

val wordCounts = textFile
  .flatMap(line => line.split("\\s+"))    // 按空白字符拆分，一行变多行
  .groupBy("value")                        // 按单词分组（value 是默认列名）
  .count()                                  // 统计每个单词出现次数
  .orderBy(desc("count"))                   // 按词频降序排列

wordCounts.show(10)
// +-------+-----+
// |  value|count|
// +-------+-----+
// |   the | 1234|
// |    a   | 987 |
// |   of   | 856 |
// +-------+-----+

// 写入 Parquet 格式（列式存储，高效压缩）
wordCounts.write.mode("overwrite").parquet("hdfs:///output/wc")
```

### 使用 RDD API 的 WordCount（面试常考）

Spark 早期（1.x 时代）只有 RDD API，虽然现在 DataFrame/Dataset 是主流，但面试中仍然经常考 RDD 版本的 WordCount，因为它能考查你对函数式编程的理解：

```scala
// RDD 版本的 WordCount — 面试经典题型
val rdd = spark.sparkContext.textFile("hdfs:///data/input.txt")

val wordCountsRDD = rdd
  .flatMap(line => line.split("\\s+"))   // 压平：一行 → 多个单词
  .map(word => (word, 1))                // 映射：单词 → (单词,1)
  .reduceByKey(_ + _)                    // 根据 key 聚合
  .sortBy(_._2, ascending = false)       // 按词频降序

wordCountsRDD.take(10).foreach(println)
// (the,1234)
// (a,987)
// (of,856)
```

> **面试点**：面试官常问："RDD 版本的 WordCount 和 DataFrame 版本的 WordCount 在性能上有什么区别？" 答案的核心是：DataFrame 版本经过 Catalyst 优化器进行了查询优化，执行效率通常更高；而 RDD 版本每一步都是精确计算，你完全控制执行流程，但缺乏自动优化。另一个区别是 DataFrame 自动做了 `sort`，而 RDD 版本只能用 `sortBy` 手动排序。

### 执行流程简析

理解 Spark 的执行流程是面试的重头戏。下面我们以上面的 DataFrame WordCount 为例，拆解它在底层的执行过程。

```
1. spark.read.textFile → 创建 DataFrame（惰性，不读取数据）
   ↓ Spark 只知道 "这里有个数据源"
2. flatMap → 算子链追加（惰性）
   ↓ Spark 继续构建逻辑执行计划（Logical Plan）
3. groupBy().count() → 触发分析（Catalyst 优化）
   ↓ Catalyst 开始做谓词下推、列剪枝等优化
4. orderBy() → 添加排序节点到逻辑计划
   ↓ 生成物理执行计划（Physical Plan），选择排序策略
5. show() → Action！→ 触发 Job → Stage → Task 执行
   ↓ 真正开始把数据从 HDFS 拉到内存中计算
```

> **面试点**：Spark 的**惰性求值**（Lazy Evaluation）是一个必考点。面试官会问 "WordCount 执行到哪一步开始真正读数据？" 答案是在调用 `show()` 之前，所有算子只是构建了一个执行计划的血缘关系（Lineage）DAG，数据并没有真正被读取或计算。只有遇到 Action 算子（如 `show()`、`count()`、`save()`）时，Driver 才会根据这个 DAG 生成物理执行计划，分发 Task 去集群上执行。

### 数据本地性（Data Locality）

实际执行时，Spark 会尽量让 Task 在数据所在的位置执行，避免网络传输：

```scala
// 可以通过 UI 或日志查看数据本地性级别
// PROCESS_LOCAL  >  NODE_LOCAL  >  RACK_LOCAL  >  ANY
```

- **PROCESS_LOCAL**：Task 和待处理数据在同一 JVM 进程中——最优
- **NODE_LOCAL**：数据在同一台机器的另一进程或另一 Executor 中——需要跨进程
- **RACK_LOCAL**：数据在同一个机架的不同机器上——需要网络传输但延迟较低
- **ANY**：数据在跨机架的远程节点上——最差

> **踩坑经验**：在实践中，如果发现 Job 执行时间异常长，很有可能是数据本地性降级了——比如 Task 被分配到没有数据副本的节点上，不得不通过网络拉取数据。解决方案通常包括调整 `spark.locality.wait`（默认 3 秒）或增加数据副本数（HDFS `dfs.replication`）。

## 小结

到了这里，你已经完成了 Spark 的第一个实战应用。让我们用一张表总结本节的关键知识点：

| 知识点 | 要点 | 面试概率 |
|--------|------|---------|
| 语言选择 | 大数据开发面 Scala，数仓面 PySpark | ⭐⭐⭐ |
| 环境搭建 | spark-shell 交互式调试，IDE + SBT 开发 | ⭐⭐ |
| 应用提交 | spark-submit 是唯一提交方式 | ⭐⭐⭐⭐ |
| 部署模式 | client 调试 / cluster 生产 | ⭐⭐⭐ |
| 统一入口 | SparkSession（2.0+ 统一入口） | ⭐⭐⭐⭐ |
| 惰性求值 | Transformation 构建 DAG，Action 才实际执行 | ⭐⭐⭐⭐⭐ |
| 资源估算 | 根据数据量和集群规模计算 Executor 配置 | ⭐⭐⭐ |
| 数据本地性 | Task 尽量在数据所在节点执行 | ⭐⭐ |

> **下一步建议**：掌握了环境搭建和基础知识后，建议你继续学习 Spark Core 的核心数据结构——RDD（弹性分布式数据集）和 DataFrame/Dataset 的原理及 API 使用，这是面试中占比最大的部分。
