# 搭建 Scala 开发环境：写出你的第一行 Scala

## 环境准备

### 1. 安装 JDK

Scala 运行在 JVM 上，需要先安装 JDK。推荐 JDK 8 或 JDK 11（企业大数据环境最常用）。

```bash
# 验证 Java 安装
java -version
# 输出示例：openjdk version "11.0.20"
```

### 2. 安装 Scala

Windows 用户下载 Scala 安装包，macOS/Linux 用包管理器：

```bash
# macOS
brew install scala

# Ubuntu/Debian
sudo apt install scala
```

验证安装：
```bash
scala -version
# Scala code runner version 2.13.12
```

### 3. 配置 sbt（Scala Build Tool）

sbt 是 Scala 的标准构建工具，类似 Java 的 Maven/Gradle。

```bash
# macOS
brew install sbt

# 验证
sbt --version
```

## 第一个 Scala 程序

### REPL 交互式环境

直接输入 `scala` 进入 REPL（Read-Eval-Print Loop），可以逐行执行代码：

```scala
scala> val greeting = "Hello, 大数据!"
val greeting: String = Hello, 大数据!

scala> println(greeting)
Hello, 大数据!

scala> 1 + 2 * 3
val res0: Int = 7
```

> 💡 REPL 是学习 Scala 的最佳工具——不用编译，写完立刻看结果，非常适合实验和面试练习。

### 完整的 Scala 源文件

创建 `HelloBigData.scala`：

```scala
object HelloBigData {
  def main(args: Array[String]): Unit = {
    println("Hello, 大数据面试备战！")

    // 简单练习：计算 1 到 100 的和
    val sum = (1 to 100).sum
    println(s"1 到 100 的和是: $sum")
  }
}
```

编译运行：
```bash
scalac HelloBigData.scala     # 编译生成 .class 文件
scala HelloBigData             # 运行
```

### sbt 项目结构

真实项目用 sbt 管理，标准目录结构：

```
my-scala-project/
├── build.sbt                  # 构建配置
├── project/
│   └── build.properties       # sbt 版本
└── src/
    ├── main/
    │   └── scala/
    │       └── Main.scala     # 源代码
    └── test/
        └── scala/
            └── MainTest.scala # 测试代码
```

`build.sbt` 示例：
```scala
name := "my-scala-project"
version := "0.1"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.0" % "provided",
  "org.scalatest" %% "scalatest" % "3.2.15" % Test
)
```

## IDE 选择

| IDE | 特点 | 推荐度 |
|-----|------|--------|
| IntelliJ IDEA (Community) + Scala 插件 | 功能最全，代码提示优秀 | ⭐⭐⭐⭐⭐ |
| VS Code + Metals 扩展 | 轻量免费，启动快 | ⭐⭐⭐⭐ |
| Eclipse + Scala IDE | 老旧，不推荐新项目 | ⭐⭐ |

> ⚠️ 大数据项目通常用 IntelliJ IDEA，它的 Scala 和 sbt 支持最成熟。

## 快速自测

在 REPL 中完成以下练习：

```scala
// 练习 1：定义一个字符串
val name = "你的名字"
println(s"你好，$name！")

// 练习 2：计算 1 到 10 的平方
(1 to 10).map(x => x * x)   // 结果：Vector(1, 4, 9, 16, 25, ...)

// 练习 3：判断长度
val str = "hello scala"
str.length                    // 结果：11
str.toUpperCase               // 结果："HELLO SCALA"
str.split(" ").toList         // 结果：List("hello", "scala")
```

**本节关键点**
- JDK → Scala → sbt 是标准工具链
- REPL 是最高效的学习方式，写一行看一行
- sbt 管理项目结构，`build.sbt` 配置依赖
- IntelliJ IDEA + Scala 插件是首选 IDE
- 大数据环境中 Scala 版本以 2.12/2.13 为主
