# Scala 在大数据中的实战：Spark 源码视角与面试要点

## Spark 中的 Scala 模式

Apache Spark 核心用 Scala 写成，其 API 设计是 Scala 语言特性的集中体现。

### RDD 操作——高阶函数在分布式环境的应用

```scala
import org.apache.spark.{SparkContext, SparkConf}

val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
val sc = new SparkContext(conf)

// 核心 API 就是你在前面学到的 map/flatMap/filter/reduceByKey
val result = sc.textFile("hdfs://data/words.txt")     // RDD[String]
  .flatMap(line => line.split(" "))                    // RDD[String]  —— flatMap！
  .map(word => (word.toLowerCase.trim, 1))            // RDD[(String, Int)] —— map！
  .filter { case (word, _) => word.nonEmpty }         // RDD[(String, Int)] —— filter！
  .reduceByKey(_ + _)                                 // RDD[(String, Int)] —— reduceByKey！
  .sortBy(_._2, ascending = false)                    // RDD[(String, Int)] —— sortBy！
  .take(10)                                           // Array[(String, Int)]

sc.stop()
```

> 💡 spark 中 `rdd.map()` 和 Scala `list.map()` **函数签名一模一样**。学会 Scala 集合操作 = 学会 Spark RDD 操作。

### 函数序列化问题

```scala
// ❌ 错误：闭包捕获了不可序列化的对象
class DataProcessor(val config: Config) extends Serializable {
  def process(rdd: RDD[String]): RDD[String] = {
    // 这会把整个 DataProcessor 对象序列化发送到各 executor
    rdd.filter(line => line.contains(config.keyword))
  }
}

// ✅ 正确：只序列化需要的数据
class DataProcessor(val config: Config) extends Serializable {
  def process(rdd: RDD[String]): RDD[String] = {
    val keyword = config.keyword  // 先捕获为局部 val
    rdd.filter(line => line.contains(keyword))
  }
}
```

## Case Class 在大数据中的角色

```scala
// case class 是 Spark 数据模型的标配
case class LogEntry(
  timestamp: Long,
  userId: String,
  action: String,
  metadata: Map[String, String]
)

// 1. 自动序列化 —— 可在 executor 间传输
// 2. 模式匹配友好 —— 易于数据处理
// 3. 结构相等 —— 方便测试和断言

// Spark 中用它定义 DataFrame schema
import spark.implicits._
val logs: Dataset[LogEntry] = spark.read
  .parquet("hdfs://data/logs/")
  .as[LogEntry]  // case class 直接作为 Dataset 类型参数
```

## 模式匹配处理异构数据

```scala
// Spark 中常需要处理多样式的输入
sealed trait DataEvent
case class ClickEvent(userId: String, url: String, ts: Long) extends DataEvent
case class PurchaseEvent(userId: String, itemId: String, amount: Double, ts: Long) extends DataEvent
case class LogoutEvent(userId: String, ts: Long) extends DataEvent

def processEvent(event: DataEvent): String = event match {
  case ClickEvent(user, url, _) =>
    s"点击事件：用户 $user 访问 $url"

  case PurchaseEvent(user, item, amount, _) if amount > 1000 =>
    s"大额购买：用户 $user 购买 $item，金额 $amount"

  case PurchaseEvent(user, item, amount, _) =>
    s"普通购买：用户 $user 购买 $item，金额 $amount"

  case LogoutEvent(user, _) =>
    s"注销事件：用户 $user 登出"
}
```

## Option 处理 null 数据

```scala
// 在实际大数据场景中，数据经常有缺失字段
case class User(id: Long, name: String, email: Option[String])

// 链式处理 Option
def sendNotification(user: User, msg: String): Unit = {
  user.email.foreach { email =>
    // 只有 email 存在时才执行
    println(s"发送邮件到 $email: $msg")
  }
}

// 默认值
val greeting = user.email
  .map(e => s"你好，$e")
  .getOrElse(s"你好，用户 ${user.id}")

// for 推导式处理多个 Option
def isValidUser(name: Option[String], email: Option[String]): Option[Boolean] = {
  for {
    n <- name if n.nonEmpty
    e <- email if e.contains("@")
  } yield true
}
```

## 隐式转换与 Spark 编码器

```scala
// Spark 的 Encoder 通过隐式参数传递
import spark.implicits._

// toDF() / as[] 依赖隐式 Encoder
val ds: Dataset[User] = Seq(
  User(1L, "张三", Some("zs@test.com"))
).toDS()  // 需要隐式 Encoder[User]

// 原理：
// implicit def newProductEncoder[T <: Product]: Encoder[T]
// 编译器自动为 case class 派生 Encoder
```

## 大数据面试 Scala 高频考点

### 1. for 推导式等价转换

面试题：「把这段 for 推导式展开为 flatMap/map/filter」

```scala
val result = for {
  i <- 1 to 10
  j <- 1 to i
  if (i + j) % 2 == 0
} yield (i, j)

// 展开：
val result2 = (1 to 10).flatMap { i =>
  (1 to i).filter { j => (i + j) % 2 == 0 }
    .map { j => (i, j) }
}
```

### 2. Option 的链式操作

```scala
def getUser(id: Long): Option[User] = ???
def getProfile(user: User): Option[Profile] = ???
def getPosts(user: User): Option[List[Post]] = ???

// 面试写法
def getUserData(id: Long): Option[(User, Profile, List[Post])] = {
  for {
    user <- getUser(id)
    profile <- getProfile(user)
    posts <- getPosts(user)
  } yield (user, profile, posts)
}
```

### 3. 闭包与序列化

面试中经常给一段 Spark 代码问有什么问题：

```scala
// 问：这段代码有什么问题？
class Analyzer(val multiplier: Int) {
  def analyze(rdd: RDD[Int]): RDD[Int] = {
    rdd.map(_ * multiplier)  // 会序列化整个 Analyzer 对象
  }
}
```

答：需要让 `Analyzer` 实现 `Serializable`，或者把 `multiplier` 提取为局部 val。

### 4. 模式匹配穷尽性

```scala
sealed trait Result
case class Success(data: String) extends Result
case class Error(code: Int, msg: String) extends Result
// 如果后续有人加了 case class Timeout(duration: Long) extends Result
// 编译器会警告所有 match 处缺少 Timeout 分支
```

### 5. 函数式 vs 命令式

面试常要求对比写法，展示函数式思维：

```scala
// 需求：从用户列表提取所有邮箱，去重，过滤空值

// 命令式
import scala.collection.mutable
val emails = mutable.Set.empty[String]
for (user <- users) {
  if (user.email != null && user.email.nonEmpty) {
    emails += user.email
  }
}

// 函数式（推荐）
val emails2 = users
  .flatMap(u => Option(u.email))
  .filter(_.nonEmpty)
  .distinct
```

## 综合实战练习

```scala
// 模拟一个简化的聚合管道（类似 Spark DataFrame API）
case class Transaction(date: String, category: String, amount: Double)

val transactions = List(
  Transaction("2024-01-01", "食品", 50.0),
  Transaction("2024-01-01", "交通", 20.0),
  Transaction("2024-01-02", "食品", 80.0),
  Transaction("2024-01-02", "食品", 30.0),
  Transaction("2024-01-03", "交通", 15.0),
)

// 按日期和类别汇总
val summary = transactions
  .groupBy(t => (t.date, t.category))
  .view
  .mapValues(_.map(_.amount).sum)
  .toSeq
  .sortBy { case ((date, cat), _) => (date, cat) }

summary.foreach { case ((date, cat), total) =>
  println(f"$date %s | $cat %s | ¥$total%.2f")
}
// 2024-01-01 | 交通 | ¥20.00
// 2024-01-01 | 食品 | ¥50.00
// 2024-01-02 | 食品 | ¥110.00
// 2024-01-03 | 交通 | ¥15.00
```

**本节关键点**
- Spark RDD 的 map/flatMap/filter/reduceByKey 直接源于 Scala 集合 API
- case class 是 Spark Dataset 的类型参数，自动序列化
- 闭包序列化是 Spark 面试必问：提取局部 val 避免序列化整个对象
- 模式匹配处理异构事件流、Option 处理缺失数据——是大数据代码的日常
- 函数式思维用链式调用取代循环和可变状态
- 面试核心：for 推导式展开、Option 链式操作、模式匹配穷尽性、闭包序列化
