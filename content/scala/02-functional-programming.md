# Scala 函数式编程核心

## 函数是头等公民

Scala 中函数是第一等公民——函数可以赋值给变量、作为参数传递、作为返回值返回。

```scala
// 函数字面量（匿名函数）
val add = (x: Int, y: Int) => x + y
add(3, 5)  // 8

// 方法转函数（eta 展开）
def multiply(x: Int, y: Int): Int = x * y
val mul = multiply _       // Int => Int => Int
val mulCurried: Int => Int => Int = multiply  // 自动柯里化
```

## 高阶函数

高阶函数接收函数作为参数或返回函数。

```scala
// 函数作为参数
def operate(x: Int, y: Int, f: (Int, Int) => Int): Int = f(x, y)
operate(3, 5, _ + _)  // 8
operate(3, 5, _ * _)  // 15

// 函数作为返回值
def makeAdder(x: Int): Int => Int = (y: Int) => x + y
val add5 = makeAdder(5)
add5(3)  // 8
```

## 柯里化（Currying）

将一个多参数函数转换为多个单参数函数链。

```scala
// 普通函数
def add(x: Int, y: Int): Int = x + y

// 柯里化版本
def addCurried(x: Int)(y: Int): Int = x + y
val add5 = addCurried(5)_
add5(3)  // 8

// 实际应用：资源管理（贷款模式）
def withResource[R <: { def close(): Unit }, T](resource: R)(f: R => T): T = {
  try f(resource)
  finally resource.close()
}

// 使用 Spark 的 RDD 资源管理
import scala.io.Source
def withFile[T](path: String)(f: Iterator[String] => T): T = {
  val source = Source.fromFile(path)
  try f(source.getLines())
  finally source.close()
}
```

## 闭包（Closure）

函数引用外部变量形成闭包：

```scala
var counter = 0
val inc = () => { counter += 1; counter }
inc()  // 1
inc()  // 2

// Spark 中闭包陷阱
val factor = 10
val rdd = sc.parallelize(1 to 100)
// factor 会被序列化发送到所有 Executor
rdd.map(_ * factor)
// ✅ factor 不大时没问题
// ❌ 如果 factor 是巨大对象，会导致序列化问题
```

> **面试重点**：Spark 中闭包变量会被序列化发送到 Executor。避免在闭包中引用不可序列化或过大的对象。

## 偏函数（PartialFunction）

只接受特定范围内的输入。

```scala
// 偏函数：只处理 Int 类型的值
val pf: PartialFunction[Any, String] = {
  case i: Int if i % 2 == 0 => s"偶数: $i"
  case i: Int  => s"奇数: $i"
}

pf.isDefinedAt("hello")  // false
pf.isDefinedAt(42)       // true
pf(42)                   // "偶数: 42"

// orElse 组合偏函数
val pf1: PartialFunction[Int, String] = { case 1 => "one" }
val pf2: PartialFunction[Int, String] = { case 2 => "two" }
val combined = pf1.orElse(pf2)

// collect = filter + map（使用偏函数）
List(1, "a", 2, "b", 3).collect { case i: Int => i * 10 }  // List(10, 20, 30)
```

## 模式匹配（Pattern Matching）

模式匹配是 Scala 最强大的特性之一，远不止 switch。

### 基本模式

```scala
val x = 42
x match {
  case 0 => "零"
  case 1 | 2 => "一或二"       // 多值匹配
  case _ if x > 10 => "大于10"  // 守卫条件
  case _ => "其他"
}
```

### 类型模式

```scala
def describe(x: Any): String = x match {
  case i: Int    => s"整数: $i"
  case s: String => s"字符串, 长度: ${s.length}"
  case list: List[_] => s"列表, 长度: ${list.size}"
  case _ => "未知类型"
}
```

### 提取器模式

```scala
// Case class 自带提取器
case class Person(name: String, age: Int)
val p = Person("张三", 25)
p match {
  case Person(n, a) if a >= 18 => s"$n 是成年人"
  case Person(n, _) => s"$n 是未成年人"
}

// 列表解构
val list = List(1, 2, 3)
list match {
  case Nil => "空列表"
  case head :: tail => s"head=$head, tail=$tail"  // head=1, tail=List(2,3)
  case head :: second :: rest => s"至少两个元素"
}
```

### Option 模式匹配

```scala
val map = Map("a" -> 1, "b" -> 2)
map.get("a") match {
  case Some(v) => s"值: $v"
  case None    => "不存在"
}

// 更常用的方式
map.getOrElse("c", 0)  // 0

// 链式处理
val result = map.get("a")
  .map(_ * 10)
  .filter(_ > 5)
  .getOrElse(0)
```

## Try / Either — 函数式错误处理

```scala
import scala.util.{Try, Success, Failure}

// Try: 捕获异常的函数式方式
def parseInt(s: String): Try[Int] = Try(s.toInt)

parseInt("42")  match {
  case Success(n)  => println(s"解析成功: $n")
  case Failure(ex) => println(s"错误: ${ex.getMessage}")
}

// Either: 更丰富的错误信息
def divide(a: Int, b: Int): Either[String, Int] =
  if (b == 0) Left("除数不能为0")
  else Right(a / b)

// for-comprehension 串联
val result = for {
  x <- parseInt("10")
  y <- parseInt("2")
  r <- divide(x, y)
} yield r
// result = Right(5)
```

## For-Comprehension（重要！）

`for` 不是循环，是 map/flatMap/filter 的语法糖：

```scala
// 这三个等价：
val nums = List(1, 2, 3)
val chars = List("a", "b")

// 写法1：for-comprehension
val result1 = for {
  n <- nums
  c <- chars
  if n % 2 == 0
} yield s"$n$c"
// List("2a", "2b")

// 写法2：flatMap + map + filter
val result2 = nums
  .filter(_ % 2 == 0)
  .flatMap(n => chars.map(c => s"$n$c"))

// 写法3：脱糖
val result3 = nums.withFilter(_ % 2 == 0).flatMap(n => chars.map(c => s"$n$c"))
```

> **Spark 实战**：Spark SQL 的 DataFrame 操作大量使用 for-comprehension 来组合多步转换。

## 面试高频考点

### Q: `val` vs `var` vs `lazy val`？

| 关键字 | 求值时机 | 可变性 |
|--------|---------|--------|
| `val` | 定义时立即 | 不可变 |
| `lazy val` | 首次访问时 | 不可变 |
| `var` | 定义时立即 | 可变 |

```scala
lazy val expensive = {
  println("正在计算...")
  Thread.sleep(1000)
  42
}
// 不访问 expensive 就不会计算
println(expensive)  // 打印 "正在计算..."，然后 42
println(expensive)  // 直接打印 42（已缓存）
```

### Q: `Option` vs `null`？

`Option` 在编译期强制处理空值情况，避免 NPE。`null` 在运行时才能发现。Spark 的 Dataset API 大量使用 `Option` 来表达可空列。

### Q: `case class` vs 普通 `class`？

Case class 自动获得：`equals/hashCode/toString/copy`、伴生对象中的 `apply/unapply`、默认不可变、支持模式匹配。

## 小结

| 概念 | Spark 中的应用 |
|------|---------------|
| 高阶函数 | RDD/Dataset 的 map/flatMap/filter 操作 |
| 柯里化 | 配置函数的参数特化 |
| 偏函数 | RDD.collect 的部分处理 |
| 模式匹配 | Case class 数据提取（项目中的数据结构） |
| For-Comprehension | DataFrame 多步操作链 |
| Option | 可空列处理（Dataset API） |
| 闭包 | RDD 闭包序列化（注意大对象） |
