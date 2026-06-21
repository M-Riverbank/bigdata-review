# 条件判断与循环：if 表达式与 for 推导式

## if/else 是表达式

在 Scala 中，`if/else` 有**返回值**，这和 Java 非常不同：

```scala
// Scala 的 if 有返回值
val x = 10
val result = if (x > 5) "大于5" else "小于等于5"
// result: String = "大于5"

// 等价于 Java 的三元运算符 ?: 但更强大
```

### if/else 作为值使用

```scala
// 可以嵌套和链式
val score = 85
val grade = if (score >= 90) "A"
            else if (score >= 80) "B"
            else if (score >= 70) "C"
            else "D"
// grade: String = "B"

// 每个分支的类型必须一致（或共同父类型）
val mixed = if (score > 60) "pass" else 0
// mixed: Any = "pass"  （String 和 Int 的共同父类型是 Any）
```

> ⚠️ 如果省略 `else`，且条件为 false，返回 `Unit`（即 `()`），类似 Java 的 void。

## while 和 do-while

和 Java 类似，但大数据场景用得少：

```scala
var i = 0
while (i < 5) {
  println(s"第 ${i + 1} 次")
  i += 1
}

i = 0
do {
  println(s"i = $i")
  i += 1
} while (i < 3)
```

> 💡 函数式编程中，`while` 很少用，优先用集合的 `foreach`、`map` 等。

## for 推导式——Scala 的灵魂

这是 Scala 最强大的控制结构，远超 Java 的 for 循环。

### 基本遍历

```scala
// 遍历集合元素
val nums = List(1, 2, 3, 4, 5)
for (n <- nums) println(n)

// 遍历 Range
for (i <- 1 to 5) println(i)        // 包含 5：1,2,3,4,5
for (i <- 1 until 5) println(i)     // 不包含 5：1,2,3,4
```

### 带守卫（guard）的 for

```scala
// 加 if 过滤条件 —— 这叫「守卫」
for (n <- 1 to 10 if n % 2 == 0) {
  println(s"$n 是偶数")
}
// 输出：2, 4, 6, 8, 10

// 多个守卫
for (n <- 1 to 30 if n % 2 == 0 if n % 3 == 0) {
  println(n)  // 6, 12, 18, 24, 30
}
```

### 嵌套生成器（多重 for）

```scala
// 扁平化双重循环
for {
  x <- 1 to 3
  y <- 1 to 2
} println(s"($x, $y)")
// (1,1) (1,2) (2,1) (2,2) (3,1) (3,2)

// 等价 Java：
// for (int x = 1; x <= 3; x++)
//   for (int y = 1; y <= 2; y++)
//     System.out.println("(" + x + "," + y + ")");
```

### yield —— for 推导式生成新集合

```scala
// 标准用法：for + yield 生成新的集合
val doubled = for (n <- (1 to 5).toList) yield n * 2
// doubled: List[Int] = List(2, 4, 6, 8, 10)

// 带守卫的 yield
val evens = for (n <- 1 to 10 if n % 2 == 0) yield n
// evens: IndexedSeq[Int] = Vector(2, 4, 6, 8, 10)

// 集合类型由原始集合决定
val list = for (n <- List(1, 2, 3)) yield n * 10
// list: List[Int] = List(10, 20, 30)

// 嵌套 yield —— 等同于 flatMap
val pairs = for {
  x <- List("a", "b")
  y <- List(1, 2)
} yield s"$x$y"
// pairs: List[String] = List("a1", "a2", "b1", "b2")
```

### 中间变量绑定

```scala
// 用 = 在推导式中引入中间变量
for {
  i <- 1 to 10
  squared = i * i        // 中间赋值
  if squared % 2 == 0
} yield s"$i² = $squared"
// 输出偶数平方
```

## 实战：用 for 推导式处理数据

```scala
// 场景：从用户列表中找出成年用户，格式化为 "姓名(年龄)"
case class User(name: String, age: Int)

val users = List(
  User("张三", 25),
  User("李四", 17),
  User("王五", 30),
  User("赵六", 15)
)

val adultNames = for {
  user <- users
  if user.age >= 18              // 守卫：只取成年
} yield s"${user.name}(${user.age}岁)"

// adultNames: List[String] = List("张三(25岁)", "王五(30岁)")
```

## for 推导式 vs map/flatMap/filter

for 推导式本质上是 map/flatMap/filter 的**语法糖**：

```scala
// 这两个完全等价
val result1 = for {
  x <- List(1, 2)
  y <- List(3, 4)
  if (x + y) % 2 == 0
} yield x * y

val result2 = List(1, 2).flatMap { x =>
  List(3, 4).filter { y => (x + y) % 2 == 0 }
    .map { y => x * y }
}
// result1 == result2 == List(3, 8)
```

| 写法 | 适用场景 |
|------|---------|
| `for` 推导式 | 多步转换，逻辑清晰 |
| `map`/`flatMap`/`filter` | 单步转换，链式调用 |

## Scala 没有 break/continue？

Scala 设计上不鼓励 `break` 和 `continue`。替代方案：

```scala
// 替代 continue：用守卫
for (n <- 1 to 10 if n % 2 == 0) println(n)

// 替代 break：用 .takeWhile
(1 to 10).takeWhile(_ < 5).foreach(println)  // 1,2,3,4

// 实在要用，引入 scala.util.control.Breaks（不推荐）
import scala.util.control.Breaks._
breakable {
  for (n <- 1 to 10) {
    if (n == 5) break()
    println(n)
  }
}
```

**本节关键点**
- Scala 的 `if/else` 有返回值，是表达式而非语句
- `for` 推导式极其强大，支持守卫、嵌套生成器、中间变量绑定
- `yield` 将 for 推导式的结果收集为新集合
- for 推导式是 map/flatMap/filter 的语法糖
- Scala 风格不用 break/continue，用守卫和 takeWhile 替代
