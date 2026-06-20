# 大数据面试备战冲刺网

大数据开发面试复习平台 — 组件教程 + 互动题库 + AI 判题。

## 项目概述

面向大数据开发岗位的面试备考工具，覆盖 9 大技术组件：

| 类别 | 组件 | 教程 | 题库 |
|------|------|------|------|
| 基础语言 | Scala | 4 篇 | 9 题 |
| Spark | Spark Core | 3 篇 | 9 题 |
| Spark | Spark SQL | 3 篇 | 9 题 |
| Spark | Spark MLlib | 1 篇 | 9 题 |
| Hadoop | HDFS | 1 篇 | 9 题 |
| Hadoop | YARN | 1 篇 | 9 题 |
| 数据仓库 | Hive | 1 篇 | 9 题 |
| NoSQL | HBase | 1 篇 | 9 题 |
| 关系库 | MySQL | 0 篇 | 9 题 |

### 三大功能模块

1. **教程**：Markdown 格式的技术文档，支持代码高亮、目录导航、学习进度追踪
2. **题库**：选择题即时判分 + 简答题 AI 判分（基于 DeepSeek）
3. **错题本**：答错的题目自动收集，支持反复练习

## 快速开始

```bash
npm install
npm run dev
```

访问 http://localhost:3000

## 环境变量

创建 `.env.local` 文件：

```
DEEPSEEK_API_KEY=your_api_key_here
```

AI 判题功能需要 DeepSeek API Key。非必填——选择题无需 API Key 也能正常使用。

## 技术栈

- **框架**: Next.js 16 (App Router + Turbopack)
- **UI**: React 19 + Tailwind CSS 4
- **图表**: Recharts
- **Markdown**: react-markdown + remark-gfm + rehype-highlight
- **图标**: Lucide React
- **AI**: DeepSeek API（兼容 OpenAI SDK）

## 项目结构

```
content/           # 教程 Markdown 文件（服务端 fs 读取）
data/              # 题库 JSON（服务端 fs 读取）
public/data/       # 题库 JSON（客户端 fetch 加载）
src/
├── app/           # Next.js App Router 页面
│   ├── [component]/        # 组件教程列表 + 文章详情
│   ├── quiz/               # 题库总览 + 答题页
│   ├── wrong/              # 错题本
│   └── api/judge/          # AI 判题接口
├── components/    # React 组件
│   ├── layout/             # 侧边栏 + 顶栏
│   ├── dashboard/          # 进度环 + 组件卡片
│   ├── tutorial/           # Markdown 渲染 + 目录
│   └── quiz/               # 选择/简答题组件
├── lib/           # 工具函数（content/quiz/deepseek/components）
└── types/         # TypeScript 类型定义
```

## 添加新内容

### 添加教程

在 `content/{category}/{component}/` 下创建 Markdown 文件：

```
content/spark/sql/04-new-topic.md
```

文件命名：`序号-标题.md`（如 `04-性能调优.md`），序号决定排列顺序。

### 添加题目

在 `data/` 下创建 JSON 文件，格式参考已有文件（如 `data/spark-sql.json`），然后拷贝到 `public/data/`：

```bash
cp data/新组件.json public/data/
```

## 脚本

```bash
npm run dev      # 开发服务器
npm run build    # 生产构建
npm run start    # 生产启动
npm run lint     # ESLint 检查
```

## 部署

项目已配置 Next.js 标准构建流程，可直接部署到 Vercel 或任何支持 Node.js 的服务器。

```bash
npm run build
npm run start
```
