# 集合操作入门：List、Set、Map 的基本操作

## Scala 集合体系一览

Scala 的集合分两大类：**可变**（mutable）和**不可变**（immutable）。默认导入的是不可变集合。

```
   Traversable
       │
   ┌───┴───┐
   │       │
 Iterable  Set
   │       │
   ├── Seq
   │    ├── List
   │    ├── Vector
   │    └── Range
   └── Map
```

> 💡 **不可变集合优先**：大数据场景中不可变性意味着线程安全、无竞态条件。

## List —— 最常用的不可变集合

```scala
// 创建 List
val emptyList = List.empty[Int]    // 空列表
val nums = List(1, 2, 3, 4, 5)
val names = List("Alice", "Bob", "Charlie")

// 用 ::(cons) 构造 —— 右结合
val list = 1 :: 2 :: 3 :: Nil      // List(1, 2, 3)

// 用 ++ 拼接
val combined = nums ++ List(6, 7, 8)  // List(1,2,3,4,5,6,7,8)
```

### List 基础操作

```scala
val list = List(10, 20, 30, 40, 50)

// 访问
list.head           // 10 —— 首元素
list.tail           // List(20, 30, 40, 50) —— 去掉首元素
list.last           // 50 —— 尾元素
list.init           // List(10, 20, 30, 40) —— 去掉尾元素
list(2)             // 30 —— 按索引访问（O(n)，慎用）
list.length         // 5
list.isEmpty        // false

// 截取
list.take(3)        // List(10, 20, 30)
list.drop(2)        // List(30, 40, 50)
list.takeRight(2)   // List(40, 50)
list.slice(1, 4)    // List(20, 30, 40)
```

## Vector —— 随机访问更快

当需要频繁随机访问时，用 Vector 代替 List（O(1) vs O(n)）：

```scala
val vec = Vector(1, 2, 3, 4, 5)
vec(2)               // 3（O(1) 随机访问）

// 追加和前置
vec :+ 6             // Vector(1, 2, 3, 4, 5, 6) —— 追加到尾
vec :+ 6 :+ 7        // Vector(1, 2, 3, 4, 5, 6, 7)
0 +: vec             // Vector(0, 1, 2, 3, 4, 5) —— 前置
```

> 💡 经验：大多数场景用 List（因为它跟函数式编程天然配合），需要大量索引访问时用 Vector。

## Set —— 不重复元素集合

```scala
val set1 = Set(1, 2, 3, 3, 4)   // 自动去重：Set(1, 2, 3, 4)

// 基础操作
set1.contains(2)        // true
set1.contains(5)        // false
set1 + 5                // Set(1, 2, 3, 4, 5) —— 添加
set1 - 3                // Set(1, 2, 4) —— 删除

// 集合运算
val setA = Set(1, 2, 3, 4)
val setB = Set(3, 4, 5, 6)

setA & setB     // Set(3, 4) —— 交集
setA | setB     // Set(1, 2, 3, 4, 5, 6) —— 并集
setA -- setB    // Set(1, 2) —— 差集
setA ++ setB    // Set(1, 2, 3, 4, 5, 6) —— 同 |
```

## Map —— 键值对

```scala
// 创建 Map
val scores = Map("张三" -> 85, "李四" -> 92, "王五" -> 78)
// 或者
val scores2 = Map(("张三", 85), ("李四", 92))

// 查询
scores("张三")                   // 85（key 不存在会抛异常）
scores.get("张三")              // Some(85) —— 推荐方式
scores.getOrElse("赵六", 0)     // 0 —— 不存在返回默认值
scores.contains("李四")         // true

// 添加和删除
scores + ("赵六" -> 88)          // 新增
scores - "王五"                  // 删除
scores ++ Map("钱七" -> 70)      // 合并

// 获取所有键值
scores.keys         // Set("张三", "李四", "王五")
scores.values       // Iterable(85, 92, 78)
```

### Map 遍历和转换

```scala
// 遍历
for ((name, score) <- scores) {
  println(s"$name: $score 分")
}

scores.foreach { case (name, score) =>
  println(s"$name: $score")
}

// map 转换
scores.map { case (k, v) => (k, v + 10) }   // 每人加 10 分
scores.filter { case (_, v) => v >= 80 }    // 及格的人
```

## Range —— 数值区间

```scala
1 to 5            // Range(1, 2, 3, 4, 5) —— 包含 5
1 until 5         // Range(1, 2, 3, 4) —— 不包含 5
1 to 10 by 2      // Range(1, 3, 5, 7, 9) —— 步长为 2
10 to 1 by -1     // Range(10, 9, 8, ..., 1) —— 反向

// Range 转为 List
(1 to 5).toList   // List(1, 2, 3, 4, 5)
```

## Tuple —— 元组

```scala
val pair = ("Scala", 2.13)            // (String, Double)
val triple = (1, "hello", true)       // (Int, String, Boolean)

// 访问元素（从 1 开始，不是 0！）
pair._1     // "Scala"
pair._2     // 2.13
triple._1   // 1
triple._3   // true

// 模式匹配解构
val (lang, version) = pair
// lang: String = "Scala"
// version: Double = 2.13

// 只有两个元素的元组有 swap
pair.swap   // (2.13, "Scala")
```

## 可变集合（mutable）

当性能需求高时使用：

```scala
import scala.collection.mutable

// 可变 ListBuffer —— 高效的追加
val buf = mutable.ListBuffer.empty[Int]
buf += 1
buf += 2
buf += 3
buf.toList          // List(1, 2, 3)

// 可变 ArrayBuffer —— 类似 Java 的 ArrayList
val arrBuf = mutable.ArrayBuffer(1, 2, 3)
arrBuf(0) = 10      // 修改元素
arrBuf += 4         // 追加

// 可变 Map
val map = mutable.Map.empty[String, Int]
map("a") = 1
map("b") = 2
map += ("c" -> 3)
```

## 集合选择速查表

| 场景 | 推荐集合 | 原因 |
|------|---------|------|
| 序列遍历 | `List` | 模式匹配友好，Head::Tail |
| 随机访问 | `Vector` | O(1) 索引访问 |
| 去重判断 | `Set` | 自动去重 |
| 键值查找 | `Map` | O(1) 查找 |
| 固定长度 | `Array` | 底层为 Java 数组，性能好 |
| 数值序列 | `Range` | 惰性求值，内存省 |
| 少量异构数据 | `Tuple` | 方便临时组合 |

**本节关键点**
- Scala 默认导入不可变集合：List、Set、Map、Vector、Range
- List 是函数式编程的主力，用 `::` 和 `Nil` 构造
- Vector 比 List 随机访问更快，List 比 Vector 模式匹配更优雅
- Set 和 Map 都有不可变/可变两套版本，默认不可变
- Tuple 元素从 `_1` 开始访问，小写数字容易搞混
