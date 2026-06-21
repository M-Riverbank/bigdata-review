# 变量和数据类型：val、var 与类型推断

## val 与 var：不可变 vs 可变

这是 Scala 最基础也是面试最爱问的概念。

```scala
// val = value，不可变（类似 Java 的 final）
val name: String = "Scala"
// name = "Java"  // ❌ 编译错误：reassignment to val

// var = variable，可变（不推荐）
var count: Int = 0
count = 1          // ✅ 可以修改
```

| 关键字 | 含义 | 可变性 | 推荐使用 |
|--------|------|--------|---------|
| `val` | 不可变值（value） | ❌ 不可重新赋值 | ✅ 优先使用 |
| `var` | 可变变量（variable） | ✅ 可重新赋值 | ⚠️ 除非必要，否则不用 |

> 💡 **函数式编程原则**：能用 `val` 绝不用 `var`。不可变性让代码线程安全、易于推理。

### lazy val：延迟初始化

```scala
// lazy val 只有在第一次被访问时才计算，且只计算一次
lazy val expensive = {
  println("计算中...")
  (1 to 1000000).sum
}

println("defined")        // 先输出 "defined"
println(expensive)        // 此时才输出 "计算中..." 然后输出 500000500050
println(expensive)        // 直接输出结果，不再计算
```

## 基本数据类型

Scala 的「基本类型」实际上是**对象**（不像 Java 有 int/Integer 之分）：

```scala
val answer: Int = 42
// 等价于：
val answer2 = 42           // 类型推断，自动识别为 Int
```

| 类型 | 说明 | 示例 |
|------|------|------|
| `Byte` | 8 位有符号整数 | `val b: Byte = 127` |
| `Short` | 16 位有符号整数 | `val s: Short = 32767` |
| `Int` | 32 位有符号整数 | `val i: Int = 42` |
| `Long` | 64 位有符号整数 | `val l: Long = 42L` |
| `Float` | 32 位浮点数 | `val f: Float = 3.14f` |
| `Double` | 64 位浮点数 | `val d: Double = 3.14` |
| `Char` | 16 位无符号 Unicode 字符 | `val c: Char = 'A'` |
| `String` | 字符串（Java String） | `val s: String = "hello"` |
| `Boolean` | 布尔值 | `val b: Boolean = true` |
| `Unit` | 类似 void，表示无返回值 | `def foo(): Unit = {}` |

## 类型推断

Scala 的类型推断很聪明——绝大多数情况下你不必写类型注解：

```scala
val num = 42               // 推断为 Int
val pi = 3.14              // 推断为 Double
val msg = "hello"          // 推断为 String
val list = List(1, 2, 3)   // 推断为 List[Int]
val map = Map("a" -> 1)    // 推断为 Map[String, Int]
```

什么时候需要显式声明类型？
```scala
// 1. 函数参数和返回值（最佳实践）
def add(a: Int, b: Int): Int = a + b

// 2. 想要更宽泛的类型
val pets: List[Any] = List("cat", 42, true)

// 3. 提高代码可读性（团队约定）
val timeout: Long = 30_000
```

## String 操作

### 字符串插值

```scala
val name = "Spark"
val version = 3.5

// s 插值器：嵌入变量
println(s"$name version $version")           // "Spark version 3.5"
println(s"${name}版本号${version}")            // "Spark版本号3.5"

// f 插值器：格式化
val pi = 3.1415926
println(f"π ≈ $pi%.2f")                     // "π ≈ 3.14"

// raw 插值器：不转义
println(raw"\n\t  <- 这些不会被转义")
```

### 常用 String 方法

```scala
val str = "Hello, Scala"

str.length              // 12
str.toLowerCase         // "hello, scala"
str.toUpperCase         // "HELLO, SCALA"
str.contains("Scala")   // true
str.startsWith("He")    // true
str.endsWith("la")      // true
str.split(",")          // Array("Hello", " Scala")
str.replace("Scala", "Spark")  // "Hello, Spark"
str.take(5)             // "Hello"
str.drop(7)             // "Scala"
str.reverse             // "alacS ,olleH"

// 去空白
"  hello  ".trim        // "hello"

// 多行字符串
val multiline =
  """第一行
    |第二行
    |第三行""".stripMargin
```

### 字符串 × 数字拼接

```scala
"a" * 3    // "aaa"  （Scala 特有！）
```

## 类型转换

```scala
// 隐式转换（自动）
val x: Int = 42
val y: Long = x          // Int → Long，自动

// 显式转换
"42".toInt               // 42
"3.14".toDouble          // 3.14
42.toString              // "42"
true.toString            // "true"
```

> ⚠️ `"abc".toInt` 会抛出 `NumberFormatException`，实际项目中用 `.toIntOption`（返回 `Option[Int]`）。

## 与 Java 类型的差异总结

| 特性 | Java | Scala |
|------|------|-------|
| 基本类型 | `int`、`long` 是原始类型 | 一切都是对象 |
| 不可变性 | 需要 `final` 关键字 | 默认 `val` |
| 字符串插值 | `"Hello " + name` | `s"Hello $name"` |
| 类型声明 | `int x = 42` | `val x = 42`（类型推断） |
| null | 常用 | 不推荐，用 `Option` |

**本节关键点**
- `val`（不可变）优先于 `var`（可变），这是函数式编程的基石
- Scala 有强大的类型推断，通常不需要显式声明类型
- 字符串插值用 `s"..."`，格式化用 `f"..."`，多行用 `"""..."""`
- `lazy val` 延迟初始化，只在首次访问时计算
- Scala 中的基础类型都是对象，与 Java 有根本不同
