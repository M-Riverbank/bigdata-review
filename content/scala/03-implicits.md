# Scala 隐式转换与隐式参数

## 隐式转换（Implicit Conversion）

隐式转换允许编译器在类型不匹配时自动插入转换函数。这是 Scala 中争议最大但威力最强的特性之一。

```scala
// 定义隐式转换
implicit def intToRichInt(i: Int): RichInt = new RichInt(i)

class RichInt(val self: Int) {
  def abs: Int = if (self < 0) -self else self
  def squared: Int = self * self
}

println(5.squared)  // 25 —— Int 本来没有 squared 方法
```

### 隐式转换的三种形式

| 形式 | 语法 | 示例 |
|------|------|------|
| 隐式方法 | `implicit def` | `implicit def strToInt(s: String): Int = s.toInt` |
| 隐式类 | `implicit class` | `implicit class RichString(s: String) { ... }` |
| 隐式值 | `implicit val` | `implicit val timeout: Int = 3000` |

### 隐式类（最常用，Scala 2.10+）

```scala
object StringOps {
  implicit class RichString(val s: String) {
    def isEmail: Boolean = s.contains("@") && s.contains(".")
    def mask: String = s.replaceAll("(?<=.{3}).(?=.{3})", "*")
  }
}

import StringOps._
println("abc@def.com".isEmail)  // true
println("13800138000".mask)     // "138****8000"
```

> **Spark 实战**：Spark 的 `rddToPairRDDFunctions` 就是隐式转换——只要 `RDD[(K, V)]`，自动获得 `reduceByKey`、`groupByKey` 等方法。

## 隐式参数（Implicit Parameters）

柯里化的最后一个参数列表可以用 `implicit` 标记，编译器自动寻找匹配的隐式值。

```scala
// 隐式参数
def greet(name: String)(implicit greeting: String): String =
  s"$greeting, $name"

// 定义隐式值
implicit val defaultGreeting: String = "你好"

println(greet("张三"))  // "你好, 张三"
println(greet("张三")("早上好"))  // "早上好, 张三"（显式覆盖）

// 实际应用：数据库连接
case class Connection(url: String)
case class Query(sql: String)

def executeQuery(query: Query)(implicit conn: Connection): String = {
  s"在 ${conn.url} 上执行: ${query.sql}"
}

implicit val defaultConn = Connection("jdbc:mysql://localhost:3306/test")
executeQuery(Query("SELECT * FROM users"))  // 自动使用 defaultConn
```

### 上下文界定（Context Bound）

```scala
// 语法糖：[T: Ordering] 等价于 (implicit ev: Ordering[T])
def max[T: Ordering](a: T, b: T): T = {
  val ordering = implicitly[Ordering[T]]
  if (ordering.gt(a, b)) a else b
}

max(3, 5)  // 5（编译器自动提供 Int 的 Ordering）
max("hello", "world")  // "world"
```

> **Spark 源码常客**：`Encoder[T]`、`ClassTag[T]` 等都是通过隐式参数传递的类型类实例。

## 隐式解析机制

编译器按以下优先级查找隐式值：

1. **当前作用域** — 直接定义的 implicit val/def
2. **显式导入** — `import xxx._`
3. **伴生对象** — 源类型或目标类型的伴生对象
4. **包对象** — 当前包的 package object

```scala
object Math {
  implicit val precision: Double = 0.001
}

import Math._
// precision 现在在作用域内

def approxEqual(x: Double, y: Double)(implicit eps: Double): Boolean =
  math.abs(x - y) < eps

approxEqual(1.001, 1.002)  // false (eps = 0.001)
```

## 类型类模式（Type Class Pattern）

Scala 用隐式参数实现 Haskell 风格的类型类：

```scala
// 1. 定义类型类
trait Show[T] {
  def show(value: T): String
}

// 2. 提供默认实例
object Show {
  implicit val intShow: Show[Int] = (value: Int) => value.toString
  implicit val stringShow: Show[String] = (value: String) => "\"" + value + "\""

  implicit def listShow[T](implicit s: Show[T]): Show[List[T]] =
    (list: List[T]) => list.map(s.show).mkString("[", ", ", "]")
}

// 3. 接口语法
object ShowSyntax {
  implicit class ShowOps[T](val value: T)(implicit s: Show[T]) {
    def show: String = s.show(value)
  }
}

import Show._
import ShowSyntax._
println(42.show)           // "42"
println(List(1, 2, 3).show)  // "[1, 2, 3]"
```

> **Spark 类型类例子**：`Encoder[T]` 的类型类——编译器为常见类型自动提供 Encoder，用户也可自定义。

## 视图界定（View Bound，已弃用）

```scala
// 已弃用，但面试可能问到
// def foo[A <% Int](a: A): Int = a  // A 必须可隐式转换为 Int
// 等价于 def foo[A](a: A)(implicit ev: A => Int): Int = a
```

## 型变与隐式

```scala
// 协变：List[Cat] 是 List[Animal] 的子类型
class Animal
class Cat extends Animal
val cats: List[Cat] = List(new Cat)
val animals: List[Animal] = cats  // ✓（List 是协变的 +T）

// 不变与隐式转关
class Box[T](val value: T)
val catBox: Box[Cat] = new Box(new Cat)
// val animalBox: Box[Animal] = catBox  // ❌ Box 是不变的

implicit def boxUpcast[T, U >: T](box: Box[T]): Box[U] = new Box(box.value)
val animalBox: Box[Animal] = catBox  // ✓ 隐式转换完成"向上转型"
```

## 面试高频考点

### Q: `implicit` 关键字的三种用法？

| 用法 | 语法 | 场景 |
|------|------|------|
| 隐式转换 | `implicit def AtoB(a: A): B` | 类型转换 |
| 隐式类 | `implicit class RichA(a: A)` | 扩展方法 |
| 隐式参数 | `def f(x: X)(implicit y: Y)` | 上下文注入 |

### Q: 隐式转换有什么风险？

1. **可读性差**：隐式转换不显式出现在代码中，增加理解难度
2. **命名冲突**：多个隐式同一类型会报错
3. **调试困难**：编译器自动插入的代码难以追踪
4. **性能开销**：隐式转换可能创建额外的中间对象

### Q: Spark 中哪些地方用了隐式转换？

```scala
import spark.implicits._
// 1. `$"col_name"` — 字符串隐式转为 Column
// 2. `.toDF()` — RDD 转为 DataFrame
// 3. `.as[T]` — Dataset 的强类型操作
// 4. `rddToPairRDDFunctions` — RDD[(K,V)] 获得 PairRDDFunctions 方法
```

### Q: 为什么 Spark 推荐用 `implicit class` 而不是 `implicit def` 做隐式转换？

`implicit class` 更安全：不需要显式定义值类、编译器优化（值类可避免装箱）、语法更清晰。

## 小结

| 核心概念 | 关键点 |
|---------|--------|
| 隐式转换 | 编译器自动类型修复，Spark 中大量使用 |
| 隐式参数 | 上下文注入，类型类模式的基石 |
| 类型类 | `Show[T]`、`Encoder[T]`、`Ordering[T]` |
| 解析顺序 | 当前作用域 → 显式导入 → 伴生对象 → 包对象 |
| Spark 实战 | `implicits._` 导入、Encoder 提供、RDD/KV 扩展 |
