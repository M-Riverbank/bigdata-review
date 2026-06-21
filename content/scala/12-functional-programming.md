# 函数式编程详解：不可变性、柯里化与偏函数

## 函数式编程核心原则

在函数式编程（FP）中，函数是一等公民，数据是不可变的。

### 1. 纯函数

**纯函数** = 相同输入永远产生相同输出 + 无副作用：

```scala
// ✅ 纯函数
def add(a: Int, b: Int): Int = a + b
def toUpper(s: String): String = s.toUpperCase

// ❌ 非纯函数（有副作用）
var counter = 0
def increment(): Int = {
  counter += 1                 // 修改外部状态
  counter
}

// ❌ 非纯函数（依赖外部状态）
def getDiscount(price: Double): Double =
  if (isHoliday) price * 0.8   // 依赖外部 isHoliday
  else price
```

> 💡 纯函数的好处：易于测试、线程安全、可缓存、易于推理。

### 2. 不可变性

```scala
// ✅ 不可变 —— 每次返回新结构
val list1 = List(1, 2, 3)
val list2 = list1 :+ 4          // list1 不变，list2 是新的

// ❌ 可变 —— 修改原结构
import scala.collection.mutable
val buf = mutable.ArrayBuffer(1, 2, 3)
buf += 4                         // 原地修改了 buf
```

Spark 大规模并行计算的基础就是不可变性——每个 RDD 转换返回新 RDD，原 RDD 不变，这样多个 task 可以安全地共享一份数据。

## 高阶函数进阶

### 函数组合

```scala
val f: Int => Int = _ * 2
val g: Int => Int = _ + 1

// 组合：先 f 再 g
val fThenG = f.andThen(g)
fThenG(5)                    // 11 = g(f(5)) = g(10) = 11

// 组合：先 g 再 f
val gThenF = f.compose(g)
gThenF(5)                    // 12 = f(g(5)) = f(6) = 12
```

### 部分应用函数

```scala
def log(level: String, msg: String): String = s"[$level] $msg"

// 部分应用：固定第一个参数
val infoLog = log("INFO", _: String)
val errorLog = log("ERROR", _: String)

infoLog("服务启动")      // "[INFO] 服务启动"
errorLog("连接超时")     // "[ERROR] 连接超时"
```

## 柯里化（Currying）

柯里化把多参数函数变成一系列单参数函数：

```scala
// 普通多参数
def add(x: Int, y: Int): Int = x + y

// 柯里化版本
def addCurried(x: Int)(y: Int): Int = x + y

// 部分应用
val add5 = addCurried(5)    // y: Int => addCurried(5)(y)
add5(3)                     // 8

// 等价于
val add5v2 = (y: Int) => add(5, y)
```

### 柯里化的实际用途

```scala
// 1. 提供更好的类型推断
def withResources[R](resource: String)(f: String => R): R = f(resource)

withResources("db://localhost") { conn =>
  // conn 的类型自动推断为 String
  println(s"已连接：$conn")
  conn.length
}

// 2. 让最后一个参数组使用 {} 而非 ()
List(1, 2, 3).foldLeft(0) { (acc, n) =>
  acc + n * n                     // 大括号里写匿名函数，更自然
}

// 3. 类似 Builder 模式
def query(db: String)(table: String)(where: String): String =
  s"SELECT * FROM $table WHERE $where"

val q = query("mydb") _
val fromUsers = q("users") _
val result = fromUsers("age > 18")
// result: "SELECT * FROM users WHERE age > 18"
```

## 闭包

闭包是一个携带了其定义环境变量的函数：

```scala
def makeMultiplier(factor: Int): Int => Int = {
  (x: Int) => x * factor     // factor 被捕获为闭包变量
}

val double = makeMultiplier(2)
val triple = makeMultiplier(3)

double(10)      // 20
triple(10)      // 30
```

```scala
// 闭包陷阱：var 变量被捕获时会改变
var count = 0
val increment = () => { count += 1; count }
increment()     // 1
increment()     // 2 —— 每次调用结果不同！（不是纯函数）
```

## 偏函数（Partial Function）

偏函数是只在部分输入上有定义的函数：

```scala
// 偏函数的定义
val sqrt: PartialFunction[Double, Double] = {
  case x if x >= 0 => Math.sqrt(x)
}

sqrt.isDefinedAt(4)     // true
sqrt.isDefinedAt(-1)    // false
sqrt(4)                 // 2.0
// sqrt(-1)             // 会抛 MatchError

// 转为普通函数
val safeSqrt: Double => Option[Double] = sqrt.lift
safeSqrt(4)             // Some(2.0)
safeSqrt(-1)            // None
```

### 偏函数实战：collect

```scala
// collect = filter + map，用偏函数同时做过滤和转换
val nums = List(1, -2, 3, -4, 5)

// 仅对正数开方
nums.collect { case x if x > 0 => Math.sqrt(x) }
// List(1.0, 1.732, 2.236)

// 等价于：
nums.filter(_ > 0).map(Math.sqrt)
```

```scala
// 解析混合列表
val mixed = List("42", "abc", "100", "def")
mixed.collect { case s if s.forall(_.isDigit) => s.toInt }
// List(42, 100) —— 一步完成过滤和转换
```

## 传名参数（ByName）

```scala
// 传名参数：每次使用都会重新计算
def byName(x: => Int): Int = x + x    // x 计算了两次

var n = 0
byName { n += 1; n }      // 3 = (0+1) + (1+1) —— 每次引用 x 都执行一次

// 传值参数：在调用前计算一次
def byValue(x: Int): Int = x + x

n = 0
byValue { n += 1; n }     // 2 = (1) + (1) —— 只计算一次
```

> 💡 传名参数常用于实现自定义控制结构和惰性求值。

## 实战：用 FP 风格统计日志

```scala
case class LogEntry(timestamp: Long, level: String, msg: String)

val logs = List(
  LogEntry(1, "INFO", "服务启动"),
  LogEntry(2, "ERROR", "DB连接失败"),
  LogEntry(3, "INFO", "请求处理"),
  LogEntry(4, "ERROR", "超时"),
  LogEntry(5, "WARN", "内存使用高"),
)

// 用纯函数式链式处理
val errorSummary = logs
  .filter(_.level == "ERROR")          // 只取 ERROR
  .map(e => s"[${e.timestamp}] ${e.msg}") // 格式化
  .mkString("\n")

// 或统计各级别数量
val levelCounts = logs
  .groupBy(_.level)
  .view
  .mapValues(_.size)
  .toMap
// Map("INFO" -> 2, "ERROR" -> 2, "WARN" -> 1)
```

## 函数式 vs 命令式

同样的任务，两种写法：

**命令式（Java 风格）**：
```scala
var result = List.empty[Int]
for (n <- nums) {
  if (n > 0) {
    result = result :+ n * 2
  }
}
// 累加结果有副作用
```

**函数式（推荐）**：
```scala
val result = nums.filter(_ > 0).map(_ * 2)
// 无副作用，一行搞定
```

> 💡 函数式代码更短、更安全、更易并行化——这正是 Scala 在大数据中流行的原因。

**本节关键点**
- 纯函数 = 确定性输出 + 无副作用，是 FP 的基石
- 柯里化让函数可以逐步接收参数，方便部分应用
- 偏函数用 `case` 定义，通过 `isDefinedAt` 判断，`lift` 转为 Option
- 传名参数 `=>` 延迟计算，适合控制结构和惰性求值
- 函数组合 `andThen` / `compose` 让函数像管道一样连接
