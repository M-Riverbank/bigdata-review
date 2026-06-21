# 函数定义与使用：掌握 def、匿名函数与高阶函数

## 函数基本定义

在 Scala 中，使用 `def` 关键字定义函数/方法：

```scala
// 最简形式
def greet(name: String): String = {
  s"你好，$name！"
}

// 单行函数可以省略花括号
def add(a: Int, b: Int): Int = a + b

// 返回值类型可以推断（推荐显式写）
def multiply(a: Int, b: Int) = a * b

// 无参数函数
def currentTime(): Long = System.currentTimeMillis()
currentTime()    // 带括号调用
currentTime      // 无副作用时可省略括号
```

### 默认参数

```scala
def connect(host: String = "localhost", port: Int = 3306): String =
  s"jdbc:mysql://$host:$port"

connect()                         // "jdbc:mysql://localhost:3306"
connect("db.example.com")         // "jdbc:mysql://db.example.com:3306"
connect(port = 5432)              // "jdbc:mysql://localhost:5432"
connect(port = 5432, host = "pg") // 命名参数，顺序可随意
```

### 变长参数

```scala
def sum(nums: Int*): Int = nums.sum

sum(1, 2, 3)          // 6
sum(1, 2, 3, 4, 5)    // 15

// 把集合传给变长参数用 :_*
val list = List(1, 2, 3)
sum(list: _*)         // 6
```

## 匿名函数（Lambda）

```scala
// 完整形式
val add = (a: Int, b: Int) => a + b

// 类型推断（需要上下文）
val nums = List(1, 2, 3)
nums.map((x: Int) => x * 2)    // 完整
nums.map(x => x * 2)           // 省略类型
nums.map(_ * 2)                // 占位符语法 —— 最简洁
```

### 占位符 `_` 用法

```scala
// _ 代表函数的每个参数，按出现顺序对应
List(1, 2, 3).reduce(_ + _)     // 等价于 reduce((a, b) => a + b)
List(1, 2, 3).filter(_ > 1)     // 等价于 filter(x => x > 1)
List("a", "b").map(_.toUpperCase) // 等价于 map(s => s.toUpperCase)

// 注意：同一个参数不能重复用 _
// nums.filter(_ > 5 && _ < 10)  // ❌ 错误！两个 _ 指不同的参数
nums.filter(x => x > 5 && x < 10)  // ✅ 正确
```

## 高阶函数

**高阶函数** = 接受函数作为参数 或 返回一个函数的函数。

### map —— 转换每个元素

```scala
val nums = List(1, 2, 3, 4, 5)
nums.map(n => n * 2)          // List(2, 4, 6, 8, 10)
nums.map(_ * 2)               // 同上，更简洁
nums.map(n => s"编号$n")      // List("编号1", ..., "编号5")
```

### filter / filterNot

```scala
nums.filter(_ % 2 == 0)       // List(2, 4) —— 保留偶数
nums.filterNot(_ % 2 == 0)    // List(1, 3, 5) —— 排除偶数
nums.filter(_ > 2)            // List(3, 4, 5)
```

### reduce —— 归约

```scala
nums.reduce(_ + _)             // 15 = ((((1+2)+3)+4)+5)
nums.reduce(_ * _)             // 120 = 5!
nums.reduceLeft((a, b) => a - b)  // -13 = ((((1-2)-3)-4)-5)
nums.reduceRight((a, b) => a - b) // 3 = 1-(2-(3-(4-5)))
```

### foreach —— 副作用操作

```scala
nums.foreach(n => println(s"处理第 $n 条"))
nums.foreach(println)          // 直接传函数引用
```

### 更多高阶函数一览

| 函数 | 作用 | 示例 |
|------|------|------|
| `map` | 转换每个元素 | `list.map(_ * 2)` |
| `flatMap` | 转换并展平 | `list.flatMap(_.split(","))` |
| `filter` | 保留满足条件的 | `list.filter(_ > 0)` |
| `reduce` | 归约为一个值 | `list.reduce(_ + _)` |
| `fold` | 带初始值的归约 | `list.fold(0)(_ + _)` |
| `foreach` | 遍历（有副作用） | `list.foreach(println)` |
| `exists` | 是否存在满足条件的 | `list.exists(_ > 10)` |
| `forall` | 是否全部满足条件 | `list.forall(_ > 0)` |
| `find` | 查找第一个满足条件的 | `list.find(_ > 5)` 返回 `Option` |
| `groupBy` | 按条件分组 | `list.groupBy(_ % 2)` |

## 部分应用函数

```scala
def multiply(a: Int, b: Int): Int = a * b

// 部分应用：固定第一个参数
val double = multiply(2, _: Int)
double(5)                       // 10
double(10)                      // 20

val triple = multiply(3, _: Int)
triple(5)                       // 15
```

## 柯里化（Currying）

```scala
// 普通函数
def add(a: Int, b: Int): Int = a + b

// 柯里化版本 —— 参数分开传递
def addCurried(a: Int)(b: Int): Int = a + b

val add5 = addCurried(5) _     // 固定第一个参数，返回函数
add5(3)                        // 8
add5(10)                       // 15

// 等价于：
val add5v2 = (b: Int) => add(5, b)
```

柯里化的一个实际用途：
```scala
// 类似 Spark 的 fold API
def fold[T](zero: T)(op: (T, T) => T): T = ???

// 使用时可以分别传入
List(1, 2, 3).foldLeft(0)(_ + _)  // 柯里化使得匿名函数可以写在大括号里
```

## 函数 vs 方法

| 对比项 | 方法（def） | 函数值（val + lambda） |
|--------|------------|---------------------|
| 定义 | `def foo(x: Int): Int = x` | `val foo = (x: Int) => x` |
| 类型 | 不是一等公民 | `Int => Int` |
| 可传递 | 需要 eta 展开（自动） | 直接作为值传递 |
| 内存 | 每次调用计算 | 存储为对象 |

```scala
// 方法自动转为函数值（eta 展开）
def double(x: Int): Int = x * 2
val func: Int => Int = double    // eta 展开，自动

// 等价于
val func2: Int => Int = (x: Int) => double(x)
```

**本节关键点**
- `def` 定义方法，支持默认参数、命名参数、变长参数
- 匿名函数 lambda 三种写法：`(x: Int) => x*2` → `x => x*2` → `_ * 2`
- 高阶函数是大数据编程的核心（map/flatMap/filter/reduce/fold）
- 柯里化把多参数函数转为单参数函数链
- 部分应用函数固定部分参数，返回新函数
