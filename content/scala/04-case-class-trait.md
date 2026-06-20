# Scala Case Class 与 Trait 详解

## Case Class

Case Class 是 Scala 中定义不可变数据模型的标准方式，也是 Spark Dataset 的强类型基础。

```scala
case class Person(name: String, age: Int, email: String = "")

// 自动获得的功能
val p = Person("张三", 25)           // 1. apply — 免 new
val p2 = p.copy(age = 26)           // 2. copy — 创建修改副本
println(p2)                          // 3. toString — "Person(张三,26,)"
p == Person("张三", 25)              // 4. equals — 结构相等（true）
Set(p, Person("张三", 25)).size     // 5. hashCode — 结构哈希（1）
p match { case Person(n, a, _) => } // 6. unapply — 模式匹配
```

### Case Class 生成的内容

```scala
// 编译器为 Case Class 生成的伴生对象：
object Person {
  // 1. apply
  def apply(name: String, age: Int, email: String = ""): Person = new Person(name, age, email)
  // 2. unapply (用于模式匹配)
  def unapply(p: Person): Option[(String, Int, String)] = Some((p.name, p.age, p.email))
  // 3. 实现 Product、Serializable 等
}

// 同时类体获得：
// - val 字段（默认不可变）
// - toString / equals / hashCode / copy
// - 默认实现了 java.io.Serializable
```

### Case Class vs 普通 Class

| 特性 | Case Class | 普通 Class |
|------|-----------|-----------|
| new 关键字 | 不需要 | 需要 |
| 字段不可变 | 默认 val | 需要显式声明 |
| equals | 结构相等 | 引用相等 |
| toString | 自动生成 | Object.toString |
| 模式匹配 | 支持（unapply） | 不支持 |
| 序列化 | 默认 Serializable | 需手动实现 |
| 作为 Map Key | 安全（hashCode 稳定） | 可能不一致 |

```scala
// ❌ 普通 class 的问题
class BadUser(val name: String, val age: Int)
Set(new BadUser("a", 1), new BadUser("a", 1)).size  // 2! 引用不等

// ✓ Case class 正确行为
case class GoodUser(name: String, age: Int)
Set(GoodUser("a", 1), GoodUser("a", 1)).size  // 1。结构相等
```

### Spark Dataset 中的 Case Class

```scala
// Case Class = Dataset 的 Schema
case class Sale(productId: Long, amount: Double, dt: String)

val ds: Dataset[Sale] = spark.read
  .option("header", "true")
  .csv("sales.csv")
  .as[Sale]  // Encoder 自动从 Case Class 推导

// 类型安全的操作
ds.filter(s => s.amount > 100)          // 编译期检查
  .map(s => (s.productId, s.amount))    // 类型安全转换
  .groupByKey(_._1)
  .agg(sum("_2").as[Double])
```

## Trait

Trait 是 Scala 的接口 + 混入（Mixin）机制，支持多重继承。

```scala
trait Logger {
  // 抽象方法
  def log(msg: String): Unit

  // 具体方法
  def info(msg: String): Unit = log(s"[INFO] $msg")
  def error(msg: String): Unit = log(s"[ERROR] $msg")
}

class ConsoleLogger extends Logger {
  override def log(msg: String): Unit = println(msg)
}
```

### Trait vs Abstract Class

| 特性 | Trait | Abstract Class |
|------|-------|---------------|
| 多重继承 | 可混入多个 | 只能继承一个 |
| 构造函数参数 | **不能有**（Scala 2 限制） | 可以有 |
| Java 互操作 | 有具体方法的 trait 编译为接口 | 编译为类 |
| 使用场景 | 行为混入 | 需要构造函数参数时 |

```scala
// 需要构造参数 → 用抽象类
abstract class Connection(val url: String, val timeout: Int) {
  def execute(sql: String): Unit
}

// 行为混入 → 用 Trait
trait AutoCloseable { self: { def close(): Unit } =>
  def withResource[T](f: => T): T = try f finally close()
}
```

### 混入（Mixin）组合

```scala
// 堆叠式修改（Stackable Modification）
trait BufferedLogger extends Logger {
  private val buf = scala.collection.mutable.ListBuffer[String]()
  abstract override def log(msg: String): Unit = {
    buf += msg
    if (buf.size >= 10) { flush(); buf.clear() }
    super.log(msg)
  }
  def flush(): Unit = buf.foreach(super.log)
}

trait TimestampedLogger extends Logger {
  abstract override def log(msg: String): Unit = {
    super.log(s"${java.time.Instant.now} $msg")
  }
}

// 混入顺序决定执行顺序（从右到左）
val logger = new ConsoleLogger with TimestampedLogger with BufferedLogger
// log 调用链：BufferedLogger → TimestampedLogger → ConsoleLogger
```

### 自身类型（Self Type）

```scala
// 确保混入时已有某个依赖
trait MetricsCollector {
  this: Logger =>  // 要求混入 MetricsCollector 的类必须也是 Logger
  def trackMetric(name: String, value: Double): Unit = {
    info(s"METRIC $name=$value")
  }
}

// ✓ 满足自身类型要求
class MonitoringService extends Logger with MetricsCollector {
  override def log(msg: String): Unit = println(msg)
}

// ❌ 编译错误：不是 Logger
// class BadService extends MetricsCollector
```

### 密封 Trait（Sealed Trait）

```scala
// 封闭继承层次 — 所有子类必须在同一文件内
sealed trait Result
case class Success(data: String) extends Result
case class Failure(reason: String) extends Result

// 完备的模式匹配 — 编译器警告缺失的 case
def handle(r: Result): String = r match {
  case Success(d) => s"OK: $d"
  case Failure(r) => s"FAIL: $r"
}
```

> **为什么用 sealed**：编译器可以做穷尽性检查（Exhaustiveness Check），忘记处理新的 case 会警告。

## 实际项目中的应用

### 1. 代数数据类型（ADT）

```scala
sealed trait Event
case class ClickEvent(userId: Long, targetId: String, timestamp: Long) extends Event
case class ViewEvent(userId: Long, pageId: String, duration: Int) extends Event
case class PurchaseEvent(userId: Long, orderId: String, amount: Double) extends Event

object EventProcessor {
  def process(event: Event): String = event match {
    case ClickEvent(uid, target, _) => s"User $uid clicked $target"
    case ViewEvent(uid, page, dur)  => s"User $uid viewed $page for ${dur}s"
    case PurchaseEvent(uid, oid, amt) => s"User $uid purchased $oid for ¥$amt"
  }
}
```

### 2. 策略模式

```scala
trait PartitionStrategy {
  def getPartition(key: Any, numPartitions: Int): Int
}

object HashPartitionStrategy extends PartitionStrategy {
  override def getPartition(key: Any, numPartitions: Int): Int =
    math.abs(key.hashCode()) % numPartitions
}

object RangePartitionStrategy extends PartitionStrategy {
  override def getPartition(key: Any, numPartitions: Int): Int =
    (key.toString.toLong % numPartitions).toInt
}
```

### 3. 依赖注入

```scala
// Trait 实现的简单 DI（不依赖框架）
trait DatabaseComponent {
  def query(sql: String): List[Map[String, String]]
}

trait AuthComponent {
  def authenticate(token: String): Boolean
}

// 服务层混入所有依赖
class UserService extends DatabaseComponent with AuthComponent {
  private val db = new MysqlDatabase  // 生产实现
  override def query(sql: String) = db.query(sql)

  private val auth = new OAuth2Auth
  override def authenticate(token: String) = auth.validate(token)

  def getUser(token: String, userId: Long): Option[String] = {
    if (authenticate(token))
      query(s"SELECT name FROM users WHERE id = $userId").headOption.map(_("name"))
    else None
  }
}
```

## 面试高频考点

### Q: Case class 和普通 class 本质的区别？

Case class 主要用于**值语义**（结构相等）、模式匹配（unapply）、不可变数据。普通 class 用于**引用语义**和可变状态。

### Q: Trait 和 Java Interface 的区别？

| 维度 | Scala Trait | Java Interface (8+) |
|------|-------------|---------------------|
| 默认方法 | ✓ | ✓ (default) |
| 字段 | ✓ | ✗ (只能 static final) |
| 访问修饰符 | 可 private/protected | 只能 public |
| 初始化顺序 | 线性化 | 单继承 |
| 多重继承 | ✓ | ✗ (类单继承) |

### Q: `sealed trait` 的作用？

封闭继承范围，编译器可做**穷尽性检查**（Pattern Match Exhaustiveness）。在 Spark 中用在状态机、事件处理等场景。

## 小结

| 概念 | 关键用途 |
|------|---------|
| Case Class | DataFrame/Dataset 的 Schema 载体，不可变数据模型 |
| Trait | 行为复用、策略模式、DI、堆叠式修改 |
| Sealed Trait | ADT、穷尽性检查、事件类型定义 |
| Self Type | 依赖声明、蛋糕模式 |
| 混入顺序 | 从右到左线性化（Linearization） |
