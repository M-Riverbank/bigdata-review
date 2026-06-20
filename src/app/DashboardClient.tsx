'use client';

import { TechComponent, UserProgress } from '@/types';
import ComponentCard from '@/components/dashboard/ComponentCard';
import ProgressRing from '@/components/dashboard/ProgressRing';
import { LayoutDashboard, BookOpen, HelpCircle, TrendingUp } from 'lucide-react';
import { useState, useEffect } from 'react';

interface ComponentData {
  component: TechComponent;
  articleCount: number;
  questionCount: number;
}

interface DashboardClientProps {
  componentData: ComponentData[];
  totalArticles: number;
  totalQuestions: number;
}

function loadProgress(): UserProgress {
  if (typeof window === 'undefined') {
    return { completedArticles: {}, quizResults: {}, wrongQuestionIds: [] };
  }
  try {
    const raw = localStorage.getItem('bigdata-review-progress');
    if (raw) return JSON.parse(raw);
  } catch {}
  return { completedArticles: {}, quizResults: {}, wrongQuestionIds: [] };
}

export default function DashboardClient({
  componentData,
  totalArticles,
  totalQuestions,
}: DashboardClientProps) {
  const [progress, setProgress] = useState<UserProgress | null>(null);

  useEffect(() => {
    setProgress(loadProgress());
  }, []);

  const completedCount = progress
    ? Object.values(progress.completedArticles).filter(Boolean).length
    : 0;
  const articleProgress = totalArticles > 0 ? (completedCount / totalArticles) * 100 : 0;

  const totalAnswered = progress ? Object.keys(progress.quizResults).length : 0;
  const totalCorrect = progress
    ? Object.values(progress.quizResults).filter(r => r.isCorrect).length
    : 0;
  const quizAccuracy = totalAnswered > 0 ? (totalCorrect / totalAnswered) * 100 : 0;

  return (
    <div>
      {/* Page header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-100 mb-2">
          <LayoutDashboard size={26} className="inline mr-2 mb-0.5 text-emerald-400" />
          导航大盘
        </h1>
        <p className="text-gray-400">大数据面试备战全线追踪，选择一个组件开始学习</p>
      </div>

      {/* Overall progress cards */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-8">
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 flex items-center gap-4">
          <div className="p-3 rounded-lg bg-emerald-500/10 text-emerald-400 border border-emerald-500/30">
            <BookOpen size={24} />
          </div>
          <div>
            <div className="text-2xl font-bold text-gray-100">{completedCount}</div>
            <div className="text-sm text-gray-500">已完成教程 / {totalArticles} 篇</div>
          </div>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 flex items-center gap-4">
          <div className="p-3 rounded-lg bg-blue-500/10 text-blue-400 border border-blue-500/30">
            <HelpCircle size={24} />
          </div>
          <div>
            <div className="text-2xl font-bold text-gray-100">{totalAnswered}</div>
            <div className="text-sm text-gray-500">已答题 / 正确 {totalCorrect} 题</div>
          </div>
        </div>

        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5 flex items-center gap-4">
          <div className="p-3 rounded-lg bg-violet-500/10 text-violet-400 border border-violet-500/30">
            <TrendingUp size={24} />
          </div>
          <div>
            <div className="text-2xl font-bold text-gray-100">
              {Math.round(quizAccuracy)}%
            </div>
            <div className="text-sm text-gray-500">答题正确率</div>
          </div>
        </div>
      </div>

      {/* Progress rings */}
      <div className="flex justify-center gap-8 mb-10">
        <ProgressRing
          value={articleProgress}
          size={100}
          color="#10b981"
          label="教程进度"
        />
        <ProgressRing
          value={quizAccuracy}
          size={100}
          color="#3b82f6"
          label="答题正确率"
        />
      </div>

      {/* Component cards grid */}
      <div>
        <h2 className="text-lg font-semibold text-gray-200 mb-4">技术组件</h2>
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {componentData.map(({ component, articleCount, questionCount }) => (
            <ComponentCard
              key={component.id}
              component={component}
              articleCount={articleCount}
              questionCount={questionCount}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
