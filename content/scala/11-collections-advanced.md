# 集合高阶操作：map、flatMap、filter、fold 实战

## 核心高阶函数速览

这些函数是 Scala 集合操作的灵魂，也是 Spark RDD/DataFrame API 的根源：

| 函数 | 签名 | 作用 | Spark 对应 |
|------|------|------|-----------|
| `map` | `F[A] => (A => B) => F[B]` | 逐元素转换 | `rdd.map()` |
| `flatMap` | `F[A] => (A => F[B]) => F[B]` | 转换 + 展平 | `rdd.flatMap()` |
| `filter` | `F[A] => (A => Boolean) => F[A]` | 保留满足条件的 | `rdd.filter()` |
| `reduce` | `F[A] => ((A,A) => A) => A` | 归约为一个值 | `rdd.reduce()` |
| `fold` | `F[A] => B => ((B,A) => B) => B` | 带初始值的归约 | `rdd.fold()` |
| `groupBy` | `F[A] => (A => K) => Map[K, F[A]]` | 按 key 分组 | `rdd.groupByKey()` |

## map —— 一对一转换

```scala
val nums = List(1, 2, 3, 4, 5)

// 三种写法等价
nums.map(n => n * 2)     // List(2, 4, 6, 8, 10)
nums.map(_ * 2)          // 同上，占位符写法
for (n <- nums) yield n * 2  // for 推导式

// 常见的 map 场景
List("hello", "world").map(_.toUpperCase)   // List("HELLO", "WORLD")
List("a", "b", "c").map(s => s"<$s>")       // List("<a>", "<b>", "<c>")
```

## flatMap —— 一对一或多，然后展平

```scala
// 经典：把 "a b" 和 "c d" 拆成单词列表
val sentences = List("hello world", "scala spark")
sentences.map(_.split(" "))       // List(Array("hello","world"), Array("scala","spark"))
sentences.flatMap(_.split(" "))   // List("hello", "world", "scala", "spark") ← 展平了！

// flatMap 本质：map + flatten
sentences.map(_.split(" ")).flatten  // 等价于 flatMap
```

### flatMap 用于过滤

```scala
// flatMap + Option：只保留 Some 的值
List("1", "abc", "2", "3").flatMap { s =>
  Try(s.toInt).toOption
}
// List(1, 2, 3) —— 自动过滤 "abc"
```

## filter / filterNot —— 保留/排除

```scala
val nums = List(1, 2, 3, 4, 5, 6)

nums.filter(_ % 2 == 0)        // List(2, 4, 6) —— 偶数
nums.filterNot(_ % 2 == 0)     // List(1, 3, 5) —— 非偶数
nums.filter(_ > 3)             // List(4, 5, 6)

// 组合
nums.filter(_ > 2).filter(_ < 5)  // List(3, 4)
nums.filter(n => n > 2 && n < 5)  // List(3, 4) —— 合并成一个条件
```

## reduce / reduceLeft / reduceRight

```scala
val nums = List(1, 2, 3, 4)

nums.reduce(_ + _)             // 10 = ((1+2)+3)+4
nums.reduceLeft(_ - _)         // -8 = (((1-2)-3)-4)
nums.reduceRight(_ - _)        // -2 = (1-(2-(3-4)))
```

> ⚠️ 空集合上 `reduce` 会抛异常。用 `reduceOption` 返回 `Option[A]`。

## fold —— 带初始值的归约

```scala
val nums = List(1, 2, 3)

// foldLeft：从左折叠，初始值 0
nums.foldLeft(0)(_ + _)        // 6 = (((0+1)+2)+3)
nums.foldLeft("")(_ + _.toString)  // "123"

// foldLeft 可以改变返回类型（非常强大！）
nums.foldLeft(List.empty[String]) { (acc, n) =>
  s"编号$n" :: acc
}.reverse   // List("编号1", "编号2", "编号3")

// foldRight：从右折叠
nums.foldRight(0)(_ - _)       // 2 = 1-(2-(3-0))
```

> 💡 `foldLeft` 和 `foldRight` 的参数顺序不同：`foldLeft` 是 `(B, A) => B`，`foldRight` 是 `(A, B) => B`。

### fold 实战：构建 Map

```scala
val pairs = List(("a", 1), ("b", 2), ("a", 3), ("c", 4), ("b", 5))

// 用 foldLeft 合并相同 key
val aggregated = pairs.foldLeft(Map.empty[String, Int]) { (acc, pair) =>
  val (key, value) = pair
  acc + (key -> (acc.getOrElse(key, 0) + value))
}
// Map("a" -> 4, "b" -> 7, "c" -> 4)
```

## groupBy —— 分组

```scala
// 按奇偶分组
val nums = List(1, 2, 3, 4, 5, 6)
nums.groupBy(_ % 2)
// Map(1 -> List(1,3,5), 0 -> List(2,4,6))

// 按首字母分组
List("apple", "ant", "banana", "bird").groupBy(_.head)
// Map('a' -> List("apple", "ant"), 'b' -> List("banana", "bird"))

// 按长度分组
List("a", "ab", "abc", "abcd").groupBy(_.length)
// Map(1->List("a"), 2->List("ab"), 3->List("abc"), 4->List("abcd"))
```

## 链式组合实战

```scala
case class Order(customer: String, amount: Double, category: String)

val orders = List(
  Order("张三", 150.0, "电子"),
  Order("李四", 80.0, "食品"),
  Order("张三", 200.0, "电子"),
  Order("王五", 50.0, "食品"),
  Order("李四", 120.0, "服装"),
)

// 需求：按客户统计总消费额，只保留 > 100 的，按金额降序排列
val bigSpenders = orders
  .groupBy(_.customer)                                // 按客户分组
  .view
  .mapValues(_.map(_.amount).sum)                     // 每组求和
  .filter { case (_, total) => total > 100 }          // 只保留 > 100 的
  .toSeq
  .sortBy { case (_, total) => -total }               // 降序排列

// bigSpenders: Seq[(String, Double)] = Seq(("张三", 350.0), ("李四", 200.0))

// 输出
bigSpenders.foreach { case (name, total) =>
  println(f"$name%-6s 总消费 ¥$total%.2f")
}
// 张三    总消费 ¥350.00
// 李四    总消费 ¥200.00
```

## sortBy / sortWith

```scala
val items = List(("a", 3), ("b", 1), ("c", 2))

items.sortBy(_._2)                 // 按第二个字段升序
// List(("b",1), ("c",2), ("a",3))

items.sortBy(_._2)(Ordering[Int].reverse)  // 降序
// List(("a",3), ("c",2), ("b",1))

items.sortWith { case ((_, v1), (_, v2)) => v1 > v2 }  // 自定义比较
```

## distinct / distinctBy

```scala
List(1, 2, 2, 3, 3, 3).distinct          // List(1, 2, 3)

// 按字段去重
case class User(id: Int, name: String)
List(User(1, "a"), User(1, "aa"), User(2, "b"))
  .distinctBy(_.id)                        // List(User(1,"a"), User(2,"b"))（保留第一个）
```

**本节关键点**
- `map`、`flatMap`、`filter` 是 Spark RDD API 的直接来源
- `flatMap` = `map` + `flatten`，既可展平也可结合 Option 过滤
- `foldLeft` 比 `reduce` 更通用——可以改变结果类型
- 链式调用 `groupBy.view.mapValues.filter.toSeq.sortBy` 完成复杂数据处理
- 所有高阶函数返回**新集合**，不修改原集合（不可变性）
