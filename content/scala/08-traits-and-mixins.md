# Trait 特质：多重继承的 Scala 之道

## Trait 是什么？

Trait（特质）是 Scala 解决代码复用的核心机制。它可以被看作「包含实现的接口」——既有 Java 接口的抽象能力，又有具体方法实现。

```scala
trait Loggable {
  // 抽象方法
  def logName: String

  // 具体方法
  def info(msg: String): Unit =
    println(s"[INFO] $logName: $msg")

  def error(msg: String): Unit =
    println(s"[ERROR] $logName: $msg")
}

class OrderService extends Loggable {
  override def logName: String = "OrderService"

  def placeOrder(): Unit = {
    info("开始下单")           // 从 trait 继承的具体方法
    // ... 下单逻辑
    info("下单成功")
  }
}
```

## 混入（Mixin）多个 Trait

一个类可以混入**多个** trait，实现类似多重继承的效果：

```scala
trait Flyable {
  def fly(): String = "我可以飞"
}

trait Swimmable {
  def swim(): String = "我可以游"
}

// 同时混入多个 trait
class Duck extends Flyable with Swimmable {
  override def toString: String = s"$fly()，$swim()"
}

val duck = new Duck
duck.fly()        // "我可以飞"
duck.swim()       // "我可以游"
```

### 混入 vs Java 接口

| 特性 | Java 接口 | Scala Trait |
|------|----------|-------------|
| 抽象方法 | ✅ | ✅ |
| 具体方法（default） | ✅（Java 8+） | ✅ |
| 构造器参数 | ❌ | ❌（Scala 3 支持） |
| 状态（字段） | ❌（只能 static final） | ✅ |
| 多重继承 | ✅ | ✅ |

## Trait 中的字段

```scala
trait Timestamped {
  val createdAt: Long = System.currentTimeMillis()  // 具体字段
  var updatedAt: Long = createdAt                   // 可变字段

  def age: Long = System.currentTimeMillis() - createdAt
}

class Post(val title: String, val content: String) extends Timestamped {
  override def toString: String = s"$title (${age}ms ago)"
}
```

## 动态混入

可以在创建对象时临时混入 trait：

```scala
class BasicCoffee {
  def cost: Double = 10.0
  def description: String = "基础咖啡"
}

trait Milk { self: BasicCoffee =>
  override def cost: Double = self.cost + 3.0
  override def description: String = self.description + "+牛奶"
}

trait Sugar { self: BasicCoffee =>
  override def cost: Double = self.cost + 1.0
  override def description: String = self.description + "+糖"
}

// 动态混入 —— 只在创建时添加
val coffee1 = new BasicCoffee with Milk
coffee1.description    // "基础咖啡+牛奶"
coffee1.cost           // 13.0

val coffee2 = new BasicCoffee with Milk with Sugar
coffee2.description    // "基础咖啡+牛奶+糖"
coffee2.cost           // 14.0
```

> 💡 这就是 Scala 著名的「蛋糕模式」（Cake Pattern）的基础——用 trait 组合功能。

## 自身类型（Self Type）

限制 trait 只能混入到特定类型的类：

```scala
// 自身类型注解：this: Database => 表示该 trait 只能混入 Database 的子类
trait Transaction { this: Database =>
  def withTransaction[T](block: => T): T = {
    begin()
    try {
      val result = block
      commit()
      result
    } catch {
      case e: Exception =>
        rollback()
        throw e
    }
  }
}

class Database {
  def begin(): Unit = println("开始事务")
  def commit(): Unit = println("提交事务")
  def rollback(): Unit = println("回滚事务")
}

// Transaction 只能混入 Database 的子类
class MySQLDB extends Database with Transaction {
  def query(sql: String): Unit = {
    withTransaction {
      println(s"执行：$sql")
    }
  }
}
```

> 💡 自身类型是 Spark 源码中的常用模式。例如 `SparkContext` 中大量 trait 都用 `self: SparkEnv =>` 约束。

## 线性化（Linearization）——方法调用顺序

当一个类的多个 trait 定义了同一个方法，调用顺序由**线性化**规则决定：

```scala
trait A { def msg: String = "A" }
trait B extends A { override def msg: String = s"B -> ${super.msg}" }
trait C extends A { override def msg: String = s"C -> ${super.msg}" }

// 线性化顺序：D → C → B → A → AnyRef → Any
class D extends B with C {
  override def msg: String = s"D -> ${super.msg}"
}

new D().msg   // "D -> C -> B -> A"
```

> ⚠️ 线性化规则：从最右 trait 开始向左回溯。`class D extends B with C` 中 C 优先级高于 B。

### 面试常考：线性化顺序

```scala
trait Base { def value: String = "Base" }
trait A extends Base { override def value: String = s"A(${super.value})" }
trait B extends Base { override def value: String = s"B(${super.value})" }

class C extends A with B {
  override def value: String = s"C(${super.value})"
}

// 问：new C().value 输出什么？
// 答："C(B(A(Base)))"
// 线性化：C → B → A → Base
```

## sealed trait —— 封闭的代数数据类型

```scala
sealed trait Option[+T]
case class Some[+T](value: T) extends Option[T]
case object None extends Option[Nothing]

// 编译器能穷尽检查
def describe(opt: Option[Int]): String = opt match {
  case Some(v) => s"有值：$v"
  case None    => "空"
}
```

## Trait vs Abstract Class

| 场景 | 用 Trait | 用 Abstract Class |
|------|---------|-------------------|
| 需要构造器参数 | ❌ | ✅ |
| 混入多个 | ✅ | ❌（只能一个） |
| 运行时动态混入 | ✅ | ❌ |
| Java 互操作 | ⚠️（有字段时小心） | ✅ |
| 作为密封类族根 | ✅（sealed trait） | ✅ |

> 💡 经验法则：不确定时优先用 trait。需要构造器参数或有 Java 互操作需求时用抽象类。

**本节关键点**
- Trait 是带实现的接口，支持抽象方法、具体方法、字段
- 动态混入 `new Class with Trait` 让对象组合极其灵活
- 自身类型 `this: Type =>` 限定 trait 的混入范围
- 线性化决定了 super 调用的方法链顺序（从右到左）
- 优先用 trait，需要构造器参数时用抽象类
