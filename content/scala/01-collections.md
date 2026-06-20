# Scala 集合操作完全指南

## Scala 集合体系概览

Scala 的集合框架是大数据开发（Spark）的基础。所有 RDD/DataFrame 操作最终都映射到 Scala 集合操作。

```
            Traversable(1)
                 │
         ┌───────┴────────┐
         │                │
       Iterable          Set
         │           ┌────┴────┐
    ┌────┴────┐   HashSet  SortedSet
    │         │
   Seq      Map
    │    ┌───┴───┐
 ┌──┴──┐ HashMap SortedMap
 │     │
List  Vector

(1) Traversable 是所有集合的顶层 trait
```

### 可变 vs 不可变

```scala
// 不可变集合（默认）— scala.collection.immutable
val list = List(1, 2, 3)
// list(0) = 5  // ❌ 编译错误

// 可变集合 — scala.collection.mutable
import scala.collection.mutable
val buf = mutable.ArrayBuffer(1, 2, 3)
buf(0) = 5  // ✓
```

> **面试重点**：Spark 中 RDD 是不可变的（immutable），这与 Scala 集合的设计哲学一致。不可变性保证了分布式计算中的数据一致性。

## 常用集合类型对比

| 集合 | 不可变版 | 可变版 | 特点 | 使用场景 |
|------|---------|--------|------|---------|
| List | `List` | `ListBuffer` | 链表，O(1) head，O(n) 随机访问 | 模式匹配、递归 |
| Vector | `Vector` | — | 宽树，O(log n) 随机访问 | 通用不可变序列 |
| ArrayBuffer | — | `ArrayBuffer` | 数组，O(1) 随机访问 | 构建大序列 |
| HashMap | `HashMap` | `HashMap` | O(1) 查找 | 键值存储 |
| HashSet | `HashSet` | `HashSet` | O(1) 查找 | 去重 |
| Range | `Range` | — | 惰性整数序列 | 循环、切片 |

## 高阶函数（面试必考）

高阶函数是函数式编程的核心，也是 Spark RDD 操作的直接来源。

### 1. map — 一对一转换

```scala
val nums = List(1, 2, 3, 4, 5)
val doubled = nums.map(_ * 2)  // List(2, 4, 6, 8, 10)
```

### 2. flatMap — 一对多展开

```scala
val sentences = List("hello world", "scala spark")
val words = sentences.flatMap(_.split(" "))  // List("hello", "world", "scala", "spark")
```

### 3. filter / filterNot — 过滤

```scala
val nums = List(1, 2, 3, 4, 5, 6)
val evens = nums.filter(_ % 2 == 0)  // List(2, 4, 6)
```

### 4. reduce / reduceLeft / reduceRight — 归约

```scala
val nums = List(1, 2, 3, 4, 5)
val sum = nums.reduce(_ + _)  // 15
// reduceLeft: ((1+2)+3)+4+5 = 15（从左结合）
// reduceRight: 1+(2+(3+(4+5))) = 15（从右结合）
```

### 5. fold / foldLeft / foldRight — 带初始值的归约

```scala
// foldLeft 从左折叠（最常用）
val nums = List(1, 2, 3)
val sum = nums.foldLeft(0)(_ + _)  // 0+1+2+3 = 6

// fold 并行时可以并行计算！（Spark aggregate 的源头）
val words = List("a", "bb", "ccc")
val totalLen = words.foldLeft(0)((acc, w) => acc + w.length)  // 6
```

### 6. groupBy — 分组

```scala
val data = List(("a", 1), ("b", 2), ("a", 3))
val grouped = data.groupBy(_._1)  // Map("a" → List(("a",1),("a",3)), "b" → List(("b",2)))
```

### 7. sortBy / sortWith — 排序

```scala
case class User(name: String, age: Int)
val users = List(User("张三", 25), User("李四", 30), User("王五", 20))

users.sortBy(_.age)        // 按 age 升序
users.sortBy(_.age)(Ordering[Int].reverse)  // 按 age 降序
users.sortWith(_.age > _.age)  // 自定义比较器
```

### 8. zip / zipWithIndex — 拉链

```scala
val a = List(1, 2, 3)
val b = List("a", "b", "c")
a.zip(b)  // List((1,"a"), (2,"b"), (3,"c"))
a.zipWithIndex  // List((1,0), (2,1), (3,0))
```

## 集合性能考虑

| 操作 | List | Vector | ArrayBuffer |
|------|------|--------|-------------|
| head | O(1) | O(log n) | O(1) |
| last | O(n) | O(log n) | O(1) |
| apply(i) | O(n) | O(log n) | O(1) |
| prepend | O(1) | O(log n) | O(n) |
| append | O(n) | O(log n) | O(1)（摊销） |

> **经验法则**：List 适合递归/模式匹配；Vector 是通用不可变序列首选；ArrayBuffer 适合构建时频繁追加。

## 与 Spark RDD 操作的对应关系

| Scala 集合 | Spark RDD/DataFrame | 语义 |
|-------------|-------------------|------|
| `map` | `rdd.map()` / `df.select()` | 逐元素转换 |
| `flatMap` | `rdd.flatMap()` / `df.explode()` | 一对多展开 |
| `filter` | `rdd.filter()` / `df.filter()` / `df.where()` | 过滤 |
| `reduce` | `rdd.reduce()` | 全局归约 |
| `fold` | `rdd.aggregate() / df.agg()` | 带初始值归约 |
| `groupBy` | `rdd.groupByKey()` / `df.groupBy()` | 分组 |
| `sortBy` | `rdd.sortBy()` / `df.orderBy()` | 排序 |
| `take` | `rdd.take()` / `df.limit()` | 取前 N 条 |
| `distinct` | `rdd.distinct()` / `df.distinct()` | 去重 |

## 面试高频考点

### Q: `map` 和 `flatMap` 的区别？

`map` 是一对一转换（T → U），`flatMap` 是一对多转换（T → Traversable[U]）并将结果展平。

```scala
// map: List[String] → List[Array[String]]
List("a b", "c d").map(_.split(" "))  // List(Array("a","b"), Array("c","d"))

// flatMap: List[String] → List[String]
List("a b", "c d").flatMap(_.split(" "))  // List("a", "b", "c", "d")
```

### Q: `reduceByKey` 为什么比 `groupByKey` 快？（Spark 语境）

`reduceByKey` 在每个分区内先做预聚合（Map 端 Combiner），大幅减少 Shuffle 的数据量。`groupByKey` 将所有数据原样 Shuffle。这也是 `foldLeft` vs `groupBy` 在单机上的类似权衡。

### Q: Scala 的 `fold` 和 `reduce` 的区别？

`reduce` 不需要初始值，元素类型不变。`fold` 需要初始值，结果类型可以不同。`fold` 可用于空集合。

## 小结

| 概念 | 记忆口诀 |
|------|---------|
| 不可变集合 | RDD 的哲学来源，分布式计算基石 |
| map/flatMap/filter | Spark 算子三部曲的原型 |
| foldLeft | 从左到右带初始值归约，Spark aggregate 的原型 |
| groupBy | 分组但不聚合，对应 Spark groupByKey |
| Vector vs List | 随机访问选 Vector，模式匹配选 List |
