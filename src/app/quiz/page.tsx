import { techComponents, getComponent } from '@/lib/components';
import { countQuestions, countChoiceQuestions, countEssayQuestions, countWritingQuestions, getQuizSet } from '@/lib/quiz';
import Link from 'next/link';
import { HelpCircle, ChevronRight, CheckCircle, XCircle, Terminal } from 'lucide-react';
import type { Metadata } from 'next';
import fs from 'fs';
import path from 'path';

export const metadata: Metadata = {
  title: '题库总览 - 大数据面试备战',
};

export default async function QuizOverviewPage() {
  // Load writing questions for Spark SQL separately
  let totalWriting = 0;
  const writingFilePath = path.join(process.cwd(), 'data', 'spark-sql-writing.json');
  if (fs.existsSync(writingFilePath)) {
    try {
      const writingRaw = fs.readFileSync(writingFilePath, 'utf-8');
      const writingData = JSON.parse(writingRaw);
      totalWriting = writingData.questions?.length || 0;
    } catch {}
  }

  // Filter components that have quiz data
  const componentsWithQuiz = techComponents.filter(c => countQuestions(c.id) > 0);

  let totalQuestions = 0;
  let totalChoice = 0;
  let totalEssay = 0;
  componentsWithQuiz.forEach(c => {
    const set = getQuizSet(c.id);
    if (set) {
      totalQuestions += set.questions.length;
      totalChoice += countChoiceQuestions(set.questions);
      totalEssay += countEssayQuestions(set.questions);
    }
  });
  totalQuestions += totalWriting;

  return (
    <div>
      {/* Header */}
      <div className="mb-8">
        <h1 className="text-2xl font-bold text-gray-100 mb-2">
          <HelpCircle size={26} className="inline mr-2 mb-0.5 text-amber-400" />
          题库总览
        </h1>
        <p className="text-gray-400">选择题即时判分，简答题和 SQL 笔试题由 DeepSeek AI 评判</p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-8">
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <div className="text-3xl font-bold text-gray-100 mb-1">{totalQuestions}</div>
          <div className="text-sm text-gray-500">总题目数</div>
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-1">
            <CheckCircle size={16} className="text-emerald-400" />
            <span className="text-3xl font-bold text-gray-100">{totalChoice}</span>
          </div>
          <div className="text-sm text-gray-500">选择题（固定答案）</div>
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-1">
            <XCircle size={16} className="text-amber-500" />
            <span className="text-3xl font-bold text-gray-100">{totalEssay}</span>
          </div>
          <div className="text-sm text-gray-500">简答题（AI 判分）</div>
        </div>
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
          <div className="flex items-center gap-2 mb-1">
            <Terminal size={16} className="text-violet-400" />
            <span className="text-3xl font-bold text-gray-100">{totalWriting}</span>
          </div>
          <div className="text-sm text-gray-500">SQL 笔试题（AI 判分）</div>
        </div>
      </div>

      {/* Component quiz cards */}
      <h2 className="text-lg font-semibold text-gray-200 mb-4">选择组件开始答题</h2>

      {componentsWithQuiz.length === 0 ? (
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-8 text-center text-gray-500">
          <HelpCircle size={40} className="mx-auto mb-3 text-gray-700" />
          <p>题库建设中，敬请期待</p>
        </div>
      ) : (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {componentsWithQuiz.map(comp => {
            const set = getQuizSet(comp.id);
            if (!set) return null;
            const choice = countChoiceQuestions(set.questions);
            const essay = countEssayQuestions(set.questions);
            const isSparkSql = comp.id === 'spark-sql';
            const targetHref = isSparkSql ? '/quiz/spark-sql' : `/quiz/${comp.id}`;

            return (
              <Link
                key={comp.id}
                href={targetHref}
                className="block bg-gray-900 border border-gray-800 rounded-xl p-5 hover:border-gray-700 hover:bg-gray-850 transition-all group"
              >
                <div className="flex items-center justify-between mb-3">
                  <h3 className="text-gray-200 font-medium group-hover:text-amber-400 transition-colors">
                    {comp.subLabel}
                    {isSparkSql && <span className="ml-2 text-[10px] text-amber-500 bg-amber-500/10 px-1.5 py-0.5 rounded-full">专页</span>}
                  </h3>
                  <ChevronRight size={16} className="text-gray-600 group-hover:text-gray-400" />
                </div>
                <div className="flex items-center gap-4 text-xs text-gray-500">
                  <span className="flex items-center gap-1">
                    <CheckCircle size={12} className="text-emerald-500" />
                    {choice} 选择题
                  </span>
                  <span className="flex items-center gap-1">
                    <XCircle size={12} className="text-amber-500" />
                    {essay} 简答题
                  </span>
                  {isSparkSql && totalWriting > 0 && (
                    <span className="flex items-center gap-1">
                      <Terminal size={12} className="text-violet-500" />
                      {totalWriting} SQL笔试题
                    </span>
                  )}
                </div>
              </Link>
            );
          })}
        </div>
      )}

      {/* Coming soon */}
      <div className="mt-8">
        <h3 className="text-sm font-medium text-gray-600 mb-3">暂无题库的组件</h3>
        <div className="flex flex-wrap gap-2">
          {techComponents
            .filter(c => countQuestions(c.id) === 0)
            .map(c => (
              <span
                key={c.id}
                className="px-3 py-1.5 text-xs rounded-lg bg-gray-900 text-gray-600 border border-gray-800"
              >
                {c.subLabel}
              </span>
            ))}
        </div>
      </div>
    </div>
  );
}
