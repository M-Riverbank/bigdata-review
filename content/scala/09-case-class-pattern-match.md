# 样本类与模式匹配：case class 和 match 表达式

## case class：一行搞定数据模型

`case class` 是 Scala 最常用的数据结构定义方式，一行代码自动生成大量方法：

```scala
// 普通 class —— 要写一大堆
class User(val name: String, val age: Int) {
  override def toString: String = s"User($name,$age)"
  override def equals(obj: Any): Boolean = ???
  override def hashCode(): Int = ???
  // ... 还有很多
}

// case class —— 一行搞定
case class User(name: String, age: Int)
```

### case class 自动生成什么？

| 方法 | 说明 |
|------|------|
| `apply` | 工厂方法，不用 `new` |
| `unapply` | 支持模式匹配提取 |
| `toString` | 可读字符串表示 |
| `equals` / `hashCode` | 基于结构相等（非引用相等） |
| `copy` | 创建副本，可修改部分字段 |
| 序列化 | 天然实现 `Serializable` |

```scala
case class Book(title: String, author: String, year: Int)

val b1 = Book("Scala编程", "Odersky", 2019)  // 自动 apply，不用 new
println(b1)              // Book(Scala编程,Odersky,2019)

val b2 = Book("Scala编程", "Odersky", 2019)
b1 == b2                 // true —— 结构相等，不是引用相等

val b3 = b1.copy(year = 2024)  // 修改 year，其余不变
// b3: Book = Book(Scala编程,Odersky,2024)
```

> ⚠️ `==` 在 Scala 中等价于 Java 的 `equals`，不是引用比较。引用比较用 `eq`。

## 模式匹配（Pattern Matching）

模式匹配是 Scala 最强大的语言特性之一，远比 Java 的 `switch` 强大：

### 基本语法

```scala
val x = 2

x match {
  case 1 => "一"
  case 2 => "二"
  case _ => "其他"     // _ 是通配符，匹配任何值
}
// res: String = "二"
```

### 类型匹配

```scala
def describe(x: Any): String = x match {
  case i: Int if i > 0  => s"正整数：$i"     // 带守卫
  case i: Int           => s"整数：$i"
  case s: String        => s"字符串：$s"
  case b: Boolean       => s"布尔值：$b"
  case _                => "未知类型"
}

describe(42)        // "正整数：42"
describe(-1)        // "整数：-1"
describe("hello")   // "字符串：hello"
```

### case class 解构

```scala
case class Person(name: String, age: Int)

val p = Person("张三", 25)

p match {
  case Person(n, a) if a >= 18 => s"$n 是成年人"
  case Person(n, a)            => s"$n 是未成年人"
}
// res: String = "张三 是成年人"
```

### 嵌套解构

```scala
case class Address(city: String, street: String)
case class Employee(name: String, address: Address)

val emp = Employee("张三", Address("北京", "中关村大街"))

emp match {
  case Employee(name, Address(city, _)) =>
    s"$name 在 $city 工作"
}
// res: String = "张三 在 北京 工作"
```

### 集合匹配

```scala
val list = List(1, 2, 3)

list match {
  case Nil              => "空列表"
  case head :: tail     => s"头部=$head, 尾部=$tail"
  case _                => "其他"
}
// res: String = "头部=1, 尾部=List(2, 3)"

// 更复杂的集合模式
List(1, 2, 3, 4, 5) match {
  case x :: y :: rest   => s"前两个：$x, $y，剩余：$rest"  // x=1, y=2, rest=List(3,4,5)
  case _                => "其他"
}

// 固定长度
List(1, 2, 3) match {
  case List(a, b, c)    => s"恰好三个：$a, $b, $c"
  case _                => "不恰好三个"
}
```

## Option 类型：告别 null

```scala
// Option 有两个子类型：Some（有值）和 None（无值）
val maybeValue: Option[String] = Some("hello")
val noValue: Option[String] = None

// 模式匹配处理 Option
maybeValue match {
  case Some(v) => println(s"有值：$v")
  case None    => println("空")
}

// Option 的常用方法
maybeValue.getOrElse("默认值")      // "hello"（有值返回自身）
noValue.getOrElse("默认值")         // "默认值"（无值返回默认值）

maybeValue.map(_.toUpperCase)       // Some("HELLO")
noValue.map(_.toUpperCase)          // None

maybeValue.filter(_.length > 10)    // None（条件不满足）
maybeValue.exists(_.length > 3)     // true
```

> 💡 在 Spark 源码中，大量 API 返回 `Option[T]` 而非 null——这是 Scala 的编码习惯。

## Either / Try —— 犯错处理

```scala
// Either：左值通常是错误，右值是成功
def divide(a: Int, b: Int): Either[String, Int] =
  if (b == 0) Left("不能除以零")
  else Right(a / b)

divide(10, 2) match {
  case Right(result) => s"结果：$result"
  case Left(error)   => s"错误：$error"
}

// Try：更简洁的异常处理
import scala.util.{Try, Success, Failure}

def safeParseInt(s: String): Try[Int] = Try(s.toInt)

safeParseInt("42") match {
  case Success(n)  => println(s"解析成功：$n")
  case Failure(ex) => println(s"解析失败：${ex.getMessage}")
}
```

## 实战：用模式匹配处理 JSON

```scala
sealed trait Json
case class JsonString(value: String) extends Json
case class JsonNumber(value: Double) extends Json
case class JsonBool(value: Boolean) extends Json
case class JsonArray(values: List[Json]) extends Json
case class JsonObject(fields: Map[String, Json]) extends Json
case object JsonNull extends Json

def render(json: Json): String = json match {
  case JsonString(v)  => s""""$v""""
  case JsonNumber(v)  => v.toString
  case JsonBool(v)    => v.toString
  case JsonNull       => "null"
  case JsonArray(vs)  => vs.map(render).mkString("[", ", ", "]")
  case JsonObject(fs) =>
    fs.map { case (k, v) => s""""$k": ${render(v)}""" }
      .mkString("{", ", ", "}")
}

val json = JsonObject(Map(
  "name" -> JsonString("Scala"),
  "age" -> JsonNumber(20),
  "active" -> JsonBool(true)
))
println(render(json))
// {"name": "Scala", "age": 20.0, "active": true}
```

**本节关键点**
- `case class` 自动生成 apply、unapply、toString、equals、hashCode、copy
- 模式匹配支持常量、变量、类型、守卫、解构、嵌套解构
- `Option[T]`（Some/None）代替 null 是 Scala 的标准做法
- `Either[L, R]` 和 `Try[T]` 提供类型安全的错误处理
- `sealed` + `case class` 让编译器能检查穷尽性，是安全 ADT 的基石
