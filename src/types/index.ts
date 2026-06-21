// 技术子组件定义
export interface TechComponent {
  id: string;              // 'spark-sql'
  category: string;        // 'spark'
  categoryLabel: string;   // 'Spark'
  subLabel: string;        // 'Spark SQL'
  description: string;
  icon: string;            // lucide icon name
  color: string;           // Tailwind color class (card top bar)
  topics: string[];
}

// 题目类型
export type Question = ChoiceQuestion | EssayQuestion | WritingQuestion;

export interface ChoiceQuestion {
  type: 'choice';
  id: string;
  difficulty: 'easy' | 'medium' | 'hard';
  stem: string;
  options: { A: string; B: string; C: string; D: string };
  answer: 'A' | 'B' | 'C' | 'D';
  explanation: string;
}

export interface EssayQuestion {
  type: 'essay';
  id: string;
  difficulty: 'easy' | 'medium' | 'hard';
  stem: string;
  referencePoints: string[];
  sampleAnswer: string;
}

// SQL 笔试题 — 表结构描述
export interface WritingTable {
  name: string;
  schema: string;   // Markdown 格式的表结构
  data?: string;    // Markdown 格式的样例数据
}

export interface WritingQuestion {
  type: 'writing';
  id: string;
  difficulty: 'easy' | 'medium' | 'hard';
  stem: string;
  tables: WritingTable[];
  referencePoints: string[];
  sampleAnswer: string;
}

// 题库集合
export interface QuizSet {
  componentId: string;
  questions: Question[];
}

// 用户进度
export interface UserProgress {
  completedArticles: Record<string, boolean>;
  quizResults: Record<string, QuizResult>;
  wrongQuestionIds: string[];
}

export interface QuizResult {
  userAnswer: string;
  isCorrect: boolean;
  aiFeedback?: string;
  score?: number;
  timestamp: number;
}

// 教程文章
export interface Article {
  slug: string;
  title: string;
  component: string;
  order: number;
  difficulty: '入门' | '基础' | '进阶' | '高阶' | '综合';
  group: string;
  groupLabel: string;
}

// AI 判题
export interface JudgeRequest {
  stem: string;
  userAnswer: string;
  referencePoints: string[];
  questionType?: 'essay' | 'writing';
  tables?: WritingTable[];
}

export interface JudgeResponse {
  score: number;
  feedback: string;
  isPass: boolean;
}
