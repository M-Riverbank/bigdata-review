# 类与对象：class、object 和 apply 方法

## class：定义类

```scala
// 最基础的类定义
class Person(val name: String, val age: Int) {
  def greet(): String = s"我叫${name}，今年${age}岁"
}

val p = new Person("张三", 25)
p.name          // "张三" —— val 参数自动生成 getter
p.greet()       // "我叫张三，今年25岁"
```

### 构造器参数：val / var / 普通

```scala
class User(id: Long, val name: String, var email: String) {
  // id: 私有参数，外部不可访问（只有类内部可见）
  // name: val 参数，自动生成 public getter
  // email: var 参数，自动生成 public getter 和 setter
}

val u = new User(1L, "alice", "alice@test.com")
// u.id       // ❌ 编译错误：id 不可访问
u.name        // ✅ "alice"
u.email       // ✅ "alice@test.com"
u.email = "new@test.com"   // ✅ var 可修改
```

### 辅助构造器

```scala
class Person(val name: String, val age: Int) {
  // 辅助构造器
  def this(name: String) = this(name, 0)     // 必须调用主构造器
  def this() = this("未命名", 0)
}

val p1 = new Person("张三", 25)
val p2 = new Person("李四")       // age = 0
val p3 = new Person()             // name = "未命名", age = 0
```

> ⚠️ 辅助构造器用得不多——默认参数通常更好用。

## object：单例对象

Scala 没有 `static` 关键字，用 `object` 代替 Java 中的静态成员。

```scala
object MathUtils {
  val PI = 3.1415926

  def square(x: Double): Double = x * x
  def cube(x: Double): Double = x * x * x
}

MathUtils.PI              // 3.1415926
MathUtils.square(3)       // 9.0
```

### 伴生对象（Companion Object）

类名和 object 名相同时，它们是**伴生关系**，可以互相访问私有成员：

```scala
class Person private (val name: String, val age: Int) {
  // private 构造器 → 只能用工厂方法创建
}

object Person {
  // 工厂方法
  def apply(name: String, age: Int): Person = new Person(name, age)
  
  // 从字符串解析
  def fromString(s: String): Person = {
    val parts = s.split(",")
    new Person(parts(0).trim, parts(1).trim.toInt)
  }
}

val p = Person("张三", 25)        // 调用 Person.apply()
val p2 = Person.fromString("李四, 30")
```

## apply 方法

`apply` 让对象可以像函数一样调用：

```scala
object Greeting {
  def apply(name: String): String = s"你好，$name！"
}

Greeting("World")         // "你好，World！" —— 调用 Greeting.apply("World")

// Array 和 List 都用这个机制
Array(1, 2, 3)            // 实际是 Array.apply(1, 2, 3)
List("a", "b")            // 实际是 List.apply("a", "b")
```

### 类的 apply

```scala
class Multiplier(factor: Int) {
  def apply(x: Int): Int = x * factor
}

val triple = new Multiplier(3)
triple(10)                // 30 —— 调用 triple.apply(10)
triple(100)               // 300
```

## 方法 vs 函数再次对比

```scala
// 对象中的方法
object Calculator {
  def add(a: Int, b: Int): Int = a + b
}

// val 赋给函数值
val addFunc: (Int, Int) => Int = Calculator.add
// 或者
val addFunc2 = Calculator.add _    // eta 展开

// List 的高阶函数其实是在接收函数值
List(1, 2, 3).map(Calculator.add(10, _))  // 部分应用
```

## 实战：构建一个简单的数据模型

```scala
// 用户模型
case class User(id: Long, name: String, email: String)

object User {
  // 工厂方法
  def create(name: String, email: String): User = {
    val id = System.currentTimeMillis()  // 简化版 ID 生成
    User(id, name, email)
  }

  // 默认头像
  def defaultAvatar(name: String): String =
    s"https://api.dicebear.com/?name=${name}"
}

// 仓库（模拟 DAO）
class UserRepository {
  private var users: List[User] = List.empty

  def add(user: User): Unit = { users = user :: users }
  def findById(id: Long): Option[User] = users.find(_.id == id)
  def findAll: List[User] = users
}

object UserRepository {
  def apply(): UserRepository = new UserRepository()
}

// 使用
val repo = UserRepository()
val user = User.create("张三", "zhangsan@test.com")
repo.add(user)
```

**本节关键点**
- `class` 定义类，构造器参数用 `val`（不可变 getter）/ `var`（可变）/ 无修饰（私有）
- `object` 是单例对象，替代 Java 的 `static` 成员
- 同名 class + object 构成伴生关系，可互访私有成员
- `apply` 方法让对象可像函数一样调用，是 Scala 的关键设计
- 工厂模式常用 `object.apply` 实现
