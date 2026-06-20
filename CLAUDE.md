# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## ⚠️ Next.js 版本警告

本项目使用 **Next.js 16.2.9**，API 和约定与旧版本存在差异。编写任何 Next.js 代码前，先查阅 `node_modules/next/dist/docs/` 中的相关指南。

## 项目简介

**大数据面试备战冲刺网** — 中文大数据面试备考平台，三大功能模块：
1. **教程**（15 篇 Markdown 技术文档）
2. **题库**（81 题，选择 + 简答，简答由 DeepSeek AI 判分）
3. **错题本**（答错自动收集，支持反复练习）

所有文本和 UI 标签为中文（`<html lang="zh-CN">`），仅暗色主题（`<html class="dark">`）。

## 常用命令

```bash
npm run dev      # 启动开发服务器 (Turbopack)
npm run build    # 生产构建
npm run start    # 启动生产服务器
npm run lint     # ESLint（core-web-vitals + typescript 规则）
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
| `/quiz/[component]` | 客户端 | 互动答题，fetch JSON 文件，简答调 `/api/judge` |
| `/wrong` | 客户端 | 错题本（LocalStorage 读取） |
| `/api/judge` | API | POST `{stem, userAnswer, referencePoints}` → 调用 DeepSeek → 返回 `{score, feedback, isPass}` |

### 数据流

- **教程数据**：`content/{category}/{sub}/` 中的 Markdown 文件 → 请求时由服务端 `fs.readFileSync` 读取
  - 命名：`01-标题.md`（序号控制排列顺序）
  - 组件 ID 映射：`spark-sql` → `content/spark/sql/`，`hdfs` → `content/hadoop/hdfs/`
- **题库数据**：
  - 服务端：`src/lib/quiz.ts` 从 `data/*.json` 读取（fs）
  - 客户端：从 `/data/*.json` fetch（`public/data/` 目录的静态资源）
  - **两个位置需要保持一致**
- **用户进度**：全部存客户端 `localStorage`，key 为 `bigdata-review-progress`，类型 `UserProgress`

### ID 到文件系统路径的映射

定义在 `src/lib/content.ts` 的 `getContentDir()` 中：
- 单词 ID：`hive` → `content/hive/`
- 带连字符的子组件：`spark-sql` → `content/spark/sql/`
- 特殊处理：`hdfs` → `content/hadoop/hdfs/`，`yarn` → `content/hadoop/yarn/`

### 关键依赖

- **AI**：`openai` SDK 指向 `https://api.deepseek.com`，使用 `DEEPSEEK_API_KEY` 环境变量
- **Markdown**：`react-markdown` + `remark-gfm` + `rehype-highlight`
- **样式**：Tailwind CSS v4（PostCSS 插件），暗色主题，自定义 `prose-content` 类
- **图标**：`lucide-react`，图标名以字符串形式存储在 `techComponents[].icon` 中

### 服务端 / 客户端边界

- 使用 React hooks、浏览器 API（localStorage、相对路径 fetch）、事件处理（onClick/onChange）的文件必须加 `'use client'`
- 服务端组件可以 `import` `@/lib/content` 和 `@/lib/quiz`（使用 `fs`）——这些导入在客户端打包时会被 tree-shake
- 模式：服务端 page.tsx 读取 FS 数据 → 通过 props 传给客户端组件

### 类型（`src/types/index.ts`）

核心类型：`TechComponent`、`Question`（`ChoiceQuestion | EssayQuestion` 的联合）、`QuizSet`、`UserProgress`、`Article`、`JudgeRequest`、`JudgeResponse`

### LocalStorage 结构

Key：`bigdata-review-progress`
```json
{
  "completedArticles": { "spark-sql/01-window-functions": true },
  "quizResults": { "ssql-c-001": { "userAnswer": "C", "isCorrect": true, "timestamp": 1234567890 } },
  "wrongQuestionIds": ["ssql-e-001"]
}
```

## 新增内容指南

### 添加教程

在 `content/{category}/{component}/` 下创建 `.md` 文件（如 `content/spark/core/04-perf-tuning.md`）。文件命名：`序号-标题.md`。

### 添加题库

在 `data/` 下创建 JSON 文件，然后**同步拷贝**到 `public/data/`：
```bash
cp data/新组件.json public/data/
```

### Next.js 16 注意事项

- 页面组件的 `params` 是 `Promise`，需要 `await`
- 使用 `use()` hook（React 19）在客户端组件中解包 params Promise
- `generateStaticParams()` 用于静态生成
