# 继承与抽象类：extends、override 与 sealed

## extends 继承

Scala 的继承语法和 Java 类似，但有重要差异：

```scala
// 基类
class Animal(val name: String) {
  def speak(): String = "..."  // 默认实现
  override def toString: String = s"动物：$name"
}

// 子类
class Dog(name: String) extends Animal(name) {
  override def speak(): String = s"$name：汪汪"
}

class Cat(name: String) extends Animal(name) {
  override def speak(): String = s"$name：喵喵"
}

val dog = new Dog("旺财")
dog.speak()   // "旺财：汪汪"
```

> ⚠️ `override` 关键字是**强制**的——防止不小心覆盖了父类方法。

## 抽象类

```scala
abstract class Shape {
  // 抽象方法 —— 没有实现
  def area: Double
  def perimeter: Double

  // 具体方法 —— 有实现
  def description: String = s"面积=${area}, 周长=${perimeter}"
}

class Circle(radius: Double) extends Shape {
  override def area: Double = Math.PI * radius * radius
  override def perimeter: Double = 2 * Math.PI * radius
}

class Rectangle(width: Double, height: Double) extends Shape {
  override def area: Double = width * height
  override def perimeter: Double = 2 * (width + height)
}
```

### 抽象字段

```scala
abstract class DatabaseConfig {
  val url: String              // 抽象字段
  val maxConnections: Int = 10 // 具体字段（有默认值）

  def connect(): String = s"连接到 $url，最大连接数 $maxConnections"
}

class MySQLConfig(host: String) extends DatabaseConfig {
  override val url: String = s"jdbc:mysql://$host:3306/mydb"
  override val maxConnections: Int = 20  // 覆盖默认值
}
```

## override 规则总结

| 被覆盖的 | 覆盖时 | 说明 |
|---------|--------|------|
| 具体方法 | 必须 `override` | 防止意外覆盖 |
| 抽象方法 | `override` 可选 | 推荐加（更清晰） |
| `val` 字段 | 必须 `override` | 可以 `override val` 重新赋值 |
| `var` 字段 | 必须 `override` | 子类可 override var |
| 无参方法 | 可用 `val` 覆盖 | `def foo: Int` → `override val foo: Int = 42` |

```scala
abstract class Parent {
  def name: String            // 无参抽象方法
  def greet: String = s"我是 $name"
}

class Child extends Parent {
  // 用 val 覆盖无参方法
  override val name: String = "小明"
}

val c = new Child
c.name     // "小明"
c.greet    // "我是 小明"
```

## 访问修饰符

```scala
class AccessDemo {
  private val secret = "私有"            // 仅本类可见
  protected val familySecret = "保护"     // 本类 + 子类可见
  val public = "公开"                     // 任何地方可见

  // private[this] —— 对象私有，同一实例内可见
  private[this] val thisOnly = "仅此实例"

  // private[package] —— 包级别私有
  private[demo] val pkgPrivate = "包内可见"
}
```

## 类型检查和转换

```scala
val animal: Animal = new Dog("旺财")

// 类型检查
animal.isInstanceOf[Dog]     // true
animal.isInstanceOf[Cat]     // false

// 类型转换（不安全，尽量用模式匹配）
animal.asInstanceOf[Dog]     // 转换为 Dog

// 获取 Class 对象
animal.getClass              // class Dog

// 更好的做法 —— 模式匹配（类型安全）
animal match {
  case d: Dog => println(s"狗：${d.speak()}")
  case c: Cat => println(s"猫：${c.speak()}")
  case _      => println("未知动物")
}
```

## sealed —— 密封类

`sealed` 限制子类必须在**同一个文件**中定义，编译器能检查模式匹配是否穷尽：

```scala
sealed abstract class Result
case class Success(data: String) extends Result
case class Failure(error: String) extends Result

def handle(r: Result): String = r match {
  case Success(data) => s"成功：$data"
  case Failure(err)  => s"失败：$err"
  // 编译器会警告：如果没有写 Failure 分支，会提示 "match may not be exhaustive"
}
```

> 💡 `sealed` 是 Scala 模式匹配安全的基石。当新增子类时，编译器会提醒所有不完整的 match 表达式。

## 实战：设计一个简单的计算模型

```scala
// 表达式建模
sealed trait Expr
case class Number(value: Double) extends Expr
case class Add(left: Expr, right: Expr) extends Expr
case class Sub(left: Expr, right: Expr) extends Expr
case class Mul(left: Expr, right: Expr) extends Expr
case class Div(left: Expr, right: Expr) extends Expr

object Calculator {
  def eval(expr: Expr): Double = expr match {
    case Number(v)    => v
    case Add(l, r)    => eval(l) + eval(r)
    case Sub(l, r)    => eval(l) - eval(r)
    case Mul(l, r)    => eval(l) * eval(r)
    case Div(l, r)    =>
      val d = eval(r)
      if (d == 0) throw new ArithmeticException("除零错误")
      else eval(l) / d
  }
}

// 使用
val expr = Add(Number(1), Mul(Number(2), Number(3)))
Calculator.eval(expr)   // 7.0
```

**本节关键点**
- `extends` 继承，`override` 必须显式声明（防止误覆盖）
- 抽象类和抽象方法用 `abstract` 关键字
- 无参方法可用 `val` 覆盖（子类可缓存计算结果）
- `sealed` 密封类限制子类范围，编译器帮你检查 match 穷尽性
- 类型判断优先用模式匹配而非 `isInstanceOf`/`asInstanceOf`
- 实战中 `sealed trait` + `case class` 组合是代数数据类型（ADT）的实现方式
