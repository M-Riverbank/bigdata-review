# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ⚠️ Next.js 版本警告

本项目使用 **Next.js 16.2.9**，API 和约定与旧版本存在差异。编写任何 Next.js 代码前，先查阅 `node_modules/next/dist/docs/` 中的相关指南。

## 项目简介

**大数据面试备战冲刺网** — 中文大数据面试备考平台，三大功能模块：
1. **教程**（Markdown 技术文档，含面经题嵌入）
2. **题库**（选择题 + 简答题 + SQL 笔试题，简答和 SQL 题由 DeepSeek AI 判分）
3. **错题本**（答错自动收集，支持反复练习）

所有文本和 UI 标签为中文（`<html lang="zh-CN">`），仅暗色主题（`<html class="dark">`）。

## 常用命令

```bash
npm run dev      # 启动开发服务器 (Turbopack)
npm run build    # 生产构建
npm run start    # 启动生产服务器
npm run lint     # ESLint 检查
```

无测试套件。

## 架构

### 路由表

| 路由 | 类型 | 说明 |
|-------|------|------|
| `/` | 混合 | 服务端 page.tsx 读取 FS 预计算统计 → 传递 props 给 DashboardClient |
| `/[component]` | 服务端 | 组件教程列表（如 `/spark-sql`），使用 `generateStaticParams` |
| `/[component]/[slug]` | 服务端 | 单篇文章（Markdown 渲染），使用 `generateStaticParams` |
| `/quiz` | 服务端 | 题库总览 |
| `/quiz/spark-sql` | 客户端 | Spark SQL 刷题专页（独占路由，优先于 `[component]` 动态路由），含难度/题型筛选 |
| `/quiz/[component]` | 客户端 | 通用答题页，fetch JSON 文件，简答调 `/api/judge` |
| `/wrong` | 客户端 | 错题本（LocalStorage 读取） |
| `/api/judge` | API | POST `{stem, userAnswer, referencePoints, questionType?, tables?}` → 调 DeepSeek → `{score, feedback, isPass}` |

> Next.js 文件系统路由优先级：`/quiz/spark-sql/page.tsx`（显式路径）优先于 `/quiz/[component]/page.tsx`（动态参数）。

### 数据流

- **教程数据**：`content/{category}/{sub}/` 中的 Markdown 文件 → 请求时由服务端 `fs.readFileSync` 读取
  - 命名：`01-标题.md`（序号控制排列顺序）
  - 组件 ID 映射：`spark-sql` → `content/spark/sql/`，`hdfs` → `content/hadoop/hdfs/`
- **面经数据**：`data/*-interview.json`（含 bindings 映射章节 ID → 题目 ID），教程页面通过 `InterviewQuestionsInline` 组件按 `component + sectionId` 加载
- **题库数据**：
  - 服务端：`src/lib/quiz.ts` 从 `data/*.json` 读取（fs）
  - 客户端：从 `/data/*.json` fetch（`public/data/` 目录的静态资源）
  - **两个位置需要保持一致** — 新增或修改 JSON 后必须 `cp data/xxx.json public/data/`
- **用户进度**：全部存客户端 `localStorage`，key 为 `bigdata-review-progress`，类型 `UserProgress`

### ID 到文件系统路径的映射

定义在 `src/lib/content.ts` 的 `getContentDir()` 中：
- 单词 ID：`hive` → `content/hive/`
- 带连字符的子组件：`spark-sql` → `content/spark/sql/`
- 特殊处理：`hdfs` → `content/hadoop/hdfs/`，`yarn` → `content/hadoop/yarn/`

### 关键依赖

- **AI**：`openai` SDK 指向 `https://api.deepseek.com`，使用 `DEEPSEEK_API_KEY` 环境变量
- **Markdown**：`react-markdown` + `remark-gfm` + `rehype-highlight` + `rehype-slug`
- **样式**：Tailwind CSS v4（PostCSS 插件），暗色主题，自定义 `prose-content` 类
- **图标**：`lucide-react`，图标名以字符串形式存储在 `techComponents[].icon` 中

### 服务端 / 客户端边界

- 使用 React hooks、浏览器 API（localStorage、相对路径 fetch）、事件处理（onClick/onChange）的文件必须加 `'use client'`
- 服务端组件可以 `import` `@/lib/content` 和 `@/lib/quiz`（使用 `fs`）——这些导入在客户端打包时会被 tree-shake
- 模式：服务端 page.tsx 读取 FS 数据 → 通过 props 传给客户端组件

### 类型（`src/types/index.ts`）

核心类型：`TechComponent`、`Question`（`ChoiceQuestion | EssayQuestion | WritingQuestion` 联合）、`QuizSet`、`UserProgress`、`Article`、`JudgeRequest`、`JudgeResponse`、`WritingTable`、`QuizResult`

### LocalStorage 结构

Key：`bigdata-review-progress`
```json
{
  "completedArticles": { "spark-sql/01-window-functions": true },
  "quizResults": { "ssql-c-001": { "userAnswer": "C", "isCorrect": true, "timestamp": 1234567890 } },
  "wrongQuestionIds": ["ssql-e-001"]
}
```

### 题库数据文件结构

JSON 分两种格式：

**普通题库**（`data/{component}.json`）：
```json
{ "componentId": "spark-sql", "questions": [ { "type": "choice"|"essay", ... } ] }
```

**面试题库**（`data/{component}-interview.json`），嵌入到教程页面中：
```json
{
  "componentId": "spark-core",
  "bindings": { "rdd-operator": ["sc-q-001", "sc-q-002"] },
  "questions": [ { "id": "sc-q-001", "type": "choice"|"essay", "source": "...", ... } ]
}
```

**SQL 笔试题**（`data/spark-sql-writing.json`），仅 Spark SQL 专页使用：
```json
{ "componentId": "spark-sql", "questions": [ { "type": "writing", "tables": [...], "sampleAnswer": "...", ... } ] }
```

### 面经嵌入机制

教程文章通过章节 ID 关联面试题。在 `ArticleContent.tsx` 中，Markdown 的 `##` 标题自动生成 `sectionId`，传递给 `InterviewQuestionsInline` 组件，后者 fetch `data/{component}-interview.json` 根据 `bindings[sectionId]` 加载对应题目。每篇文章只能绑定一个面试题库文件。

`InterviewQuestionsInline` 嵌在 `ArticleContent.tsx` 的每个 `##` 章节末尾，由 `InterviewSidebar.tsx` 提供右侧边栏导航。

## 新增内容指南

### 添加教程

在 `content/{category}/{component}/` 下创建 `.md` 文件（如 `content/spark/core/04-perf-tuning.md`）。文件命名：`序号-标题.md`。

若要让面经题出现在新章节中，需在对应 `data/{component}-interview.json` 的 `bindings` 中添加 `"章节-slug": ["题目ID"]` 映射。

### 添加题库

1. 在 `data/` 下创建 JSON 文件（格式参考已有文件）
2. 同步拷贝到 `public/data/`：
```bash
cp data/新文件.json public/data/
```

### 添加 SQL 笔试题

编辑 `data/spark-sql-writing.json`，在 `questions` 数组中添加 `type: "writing"` 的对象，包含 `tables`（表结构 Markdown 数组）、`referencePoints`、`sampleAnswer`。修改后同样 `cp` 到 `public/data/`。

### Next.js 16 注意事项

- 页面组件的 `params` 是 `Promise`，需要 `await`
- 使用 `use()` hook（React 19）在客户端组件中解包 params Promise
- `generateStaticParams()` 用于静态生成
