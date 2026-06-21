# 隐式转换与隐式参数：implicit 的原理与应用

## 隐式转换初探

`implicit` 关键词让编译器在需要类型匹配时自动「补全」转换：

```scala
// 定义一个隐式转换
implicit def intToString(x: Int): String = x.toString

// 直接使用 —— 编译器自动调用 intToString
val s: String = 42          // 等价于 val s: String = intToString(42)
println(s)                  // "42"
```

> ⚠️ 隐式转换虽强，但过度使用会让代码难以理解。现代 Scala 推荐 `given`/`using`（Scala 3 语法）。

## 隐式转换的三大使用场景

### 场景 1：扩展方法（Enrichment / Pimp My Library）

这是最常用的场景——给已有类型添加新方法：

```scala
// 给 String 添加新方法
implicit class StringOps(val s: String) {
  def isAllDigits: Boolean = s.forall(_.isDigit)
  def toOption: Option[String] = if (s.isEmpty) None else Some(s)
}

"12345".isAllDigits      // true —— 编译器自动包装为 StringOps("12345")
"abc".isAllDigits        // false
"".toOption              // None
"hello".toOption         // Some("hello")
```

```scala
// 给 Int 添加 Spark 式操作
implicit class RichInt(val n: Int) {
  def times[T](f: => T): Unit = for (_ <- 1 to n) f
  def seconds: Long = n * 1000L
}

3.times { println("Hello") }   // 打印 3 次
5.seconds                      // 5000L
```

### 场景 2：隐式参数

```scala
// 隐式参数 —— 编译器自动从上下文中查找合适的值
case class Config(host: String, port: Int)

def connect(implicit cfg: Config): String =
  s"连接到 ${cfg.host}:${cfg.port}"

// 需要有一个隐式值在作用域中
implicit val defaultConfig: Config = Config("localhost", 5432)

// 调用时不需要显式传参
connect()                      // "连接到 localhost:5432"

// 也可以显式覆盖
connect(Config("prod.db", 3306))  // "连接到 prod.db:3306"
```

> 💡 Spark 的 `SparkContext` 在很多 API 中被当做隐式参数传递。

### 场景 3：类型类（Type Class）模式

```scala
// 定义一个类型类 trait
trait JsonSerializer[T] {
  def serialize(value: T): String
}

// 为不同类型提供实例
implicit val intSerializer: JsonSerializer[Int] =
  (value: Int) => value.toString

implicit val stringSerializer: JsonSerializer[String] =
  (value: String) => s""""$value""""

implicit def listSerializer[T](implicit
  elemSer: JsonSerializer[T]
): JsonSerializer[List[T]] =
  (list: List[T]) => list.map(elemSer.serialize).mkString("[", ", ", "]")

// 使用隐式参数
def toJson[T](value: T)(implicit ser: JsonSerializer[T]): String =
  ser.serialize(value)

toJson(42)                          // "42"
toJson("hello")                     // ""hello""
toJson(List(1, 2, 3))              // "[1, 2, 3]"
toJson(List(List("a", "b")))       // "[["a", "b"]]"
```

## 隐式解析规则

编译器查找隐式值的优先顺序：

```scala
// 1. 当前作用域中定义的
implicit val ec: ExecutionContext = ExecutionContext.global

// 2. 伴生对象中定义的
case class Person(name: String)
object Person {
  implicit val defaultOrdering: Ordering[Person] =
    Ordering.by(_.name)
}

// 3. 导入的
import scala.concurrent.ExecutionContext.Implicits.global
```

> ⚠️ 如果找到多个匹配的隐式值，编译器会报错「ambiguous implicit values」。

## Context Bound（上下文界定）

```scala
// 写法 1：隐式参数（显式）
def max1[T](a: T, b: T)(implicit ord: Ordering[T]): T =
  if (ord.gt(a, b)) a else b

// 写法 2：Context Bound（语法糖）
def max2[T: Ordering](a: T, b: T): T = {
  val ord = implicitly[Ordering[T]]  // 获取隐式值
  if (ord.gt(a, b)) a else b
}

// 使用
max1(10, 5)    // 10（编译器自动提供 Int 的 Ordering）
max2(10, 5)    // 10
```

## Spark 源码中的隐式转换

Spark 核心类型 `RDD` 的隐式转换：

```scala
// 原理：RDD 本身没有 toDF() 方法，但通过隐式转换获得
import spark.implicits._

// 这行导入做了什么？
// 1. implicit def rddToDatasetHolder[T](rdd: RDD[T]): DatasetHolder[T]
// 2. implicit def localSeqToDatasetHolder[T](s: Seq[T]): DatasetHolder[T]
// 3. implicit def Encoder[T] ...

val df = Seq(1, 2, 3).toDF("id")  // toDF 是通过隐式转换获得的
```

## 隐式转换的注意事项

| 建议 | 说明 |
|------|------|
| 尽量用隐式类代替隐式函数 | `implicit class` 编译器有额外检测 |
| 不要滥用 | 隐式转换让代码难以定位问题 |
| 类型尽量窄 | `implicit def stringToInt` 比 `anyToString` 安全 |
| 明确命名 | 便于手动排除时使用 |

## Ordering 实战：自定义排序

```scala
case class Student(name: String, score: Int, grade: String)

val students = List(
  Student("张三", 85, "B"),
  Student("李四", 92, "A"),
  Student("王五", 78, "C"),
)

// 隐式提供排序规则
implicit val studentOrdering: Ordering[Student] =
  Ordering.by[Student, Int](_.score).reverse  // 按分数降序

students.sorted   // 自动使用隐式 Ordering
// List(Student("李四",92,"A"), Student("张三",85,"B"), Student("王五",78,"C"))

// 或者显式指定
students.sortBy(_.score)(Ordering[Int].reverse)
```

**本节关键点**
- 隐式转换让编译器自动补全类型转换，核心用途：扩展方法、隐式参数、类型类
- `implicit class` 是最推荐的使用方式（给已有类型添加方法）
- 隐式参数让上下文信息自动传递（类似依赖注入）
- Context Bound `[T: Ordering]` 是隐式参数的语法糖
- Spark 的 `import spark.implicits._` 大量使用隐式转换
- 适度使用，不要滥用
