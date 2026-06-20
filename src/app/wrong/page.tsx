'use client';

import { useState, useEffect } from 'react';
import { techComponents, getComponent } from '@/lib/components';
import { ChoiceQuestion, EssayQuestion, Question, UserProgress, QuizResult } from '@/types';
import Link from 'next/link';
import { XCircle, RefreshCw, Trash2, ArrowRight, BookOpen } from 'lucide-react';

function loadProgress(): UserProgress {
  try {
    const raw = localStorage.getItem('bigdata-review-progress');
    if (raw) return JSON.parse(raw);
  } catch {}
  return { completedArticles: {}, quizResults: {}, wrongQuestionIds: [] };
}

function saveProgress(progress: UserProgress) {
  localStorage.setItem('bigdata-review-progress', JSON.stringify(progress));
}

interface WrongQuestionWithMeta {
  questionId: string;
  componentId: string;
  question: Question;
  result: QuizResult;
}

export default function WrongPage() {
  const [wrongQuestions, setWrongQuestions] = useState<WrongQuestionWithMeta[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    loadWrongQuestions();
  }, []);

  const loadWrongQuestions = async () => {
    setLoading(true);
    const progress = loadProgress();
    const wrongIds = progress.wrongQuestionIds;
    const results = progress.quizResults;

    const items: WrongQuestionWithMeta[] = [];

    // Load quiz data for each component with wrong answers
    const loadedComponents = new Set<string>();
    for (const id of wrongIds) {
      const result = results[id];
      if (!result) continue;

      // Try to find the component by checking all quiz files
      for (const comp of techComponents) {
        if (loadedComponents.has(comp.id)) continue;
        try {
          const res = await fetch(`/data/${comp.id}.json`);
          if (!res.ok) continue;
          loadedComponents.add(comp.id);
          const data = await res.json();
          const found = (data.questions as Question[]).find(q => q.id === id);
          if (found) {
            items.push({
              questionId: id,
              componentId: comp.id,
              question: found,
              result,
            });
            break;
          }
        } catch {}
      }
    }

    setWrongQuestions(items);
    setLoading(false);
  };

  const clearWrong = (id: string) => {
    const progress = loadProgress();
    progress.wrongQuestionIds = progress.wrongQuestionIds.filter(wid => wid !== id);
    saveProgress(progress);
    setWrongQuestions(prev => prev.filter(w => w.questionId !== id));
  };

  const clearAll = () => {
    if (confirm('确定要清空所有错题记录吗？')) {
      const progress = loadProgress();
      progress.wrongQuestionIds = [];
      saveProgress(progress);
      setWrongQuestions([]);
    }
  };

  return (
    <div className="max-w-3xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-8">
        <div>
          <h1 className="text-2xl font-bold text-gray-100 mb-2">
            <XCircle size={26} className="inline mr-2 mb-0.5 text-amber-400" />
            错题本
          </h1>
          <p className="text-gray-400">答错的题目都在这里，反复练习直到掌握</p>
        </div>
        {wrongQuestions.length > 0 && (
          <button
            onClick={clearAll}
            className="flex items-center gap-2 px-4 py-2 rounded-lg border border-red-500/30 bg-red-500/5 text-red-400 text-sm hover:bg-red-500/10 transition-colors"
          >
            <Trash2 size={15} />
            清空全部
          </button>
        )}
      </div>

      {loading ? (
        <div className="text-center py-16 text-gray-500">加载中...</div>
      ) : wrongQuestions.length === 0 ? (
        <div className="text-center py-16">
          <XCircle size={48} className="mx-auto mb-4 text-gray-700" />
          <p className="text-gray-500 mb-4">暂无错题，继续保持！</p>
          <Link
            href="/quiz"
            className="inline-flex items-center gap-2 px-5 py-2.5 bg-emerald-600 text-white rounded-lg text-sm hover:bg-emerald-500 transition-colors"
          >
            去答题
            <ArrowRight size={16} />
          </Link>
        </div>
      ) : (
        <div className="space-y-4">
          {wrongQuestions.map((item, idx) => {
            const comp = getComponent(item.componentId);
            return (
              <div
                key={item.questionId}
                className="bg-gray-900 border border-gray-800 rounded-xl p-5"
              >
                {/* Header */}
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    {comp && (
                      <Link
                        href={`/${comp.id}`}
                        className="text-xs px-2 py-0.5 rounded-full bg-gray-800 text-gray-400 hover:text-gray-200 transition-colors"
                      >
                        {comp.subLabel}
                      </Link>
                    )}
                    <span className="text-xs text-gray-600">
                      {item.question.difficulty === 'easy'
                        ? '简单'
                        : item.question.difficulty === 'medium'
                          ? '中等'
                          : '困难'}
                    </span>
                  </div>
                  <button
                    onClick={() => clearWrong(item.questionId)}
                    className="text-gray-600 hover:text-gray-400 transition-colors"
                  >
                    <Trash2 size={14} />
                  </button>
                </div>

                {/* Stem */}
                <p className="text-gray-200 mb-3 text-sm leading-relaxed">
                  {item.question.stem}
                </p>

                {/* Answer info */}
                <div className="flex items-center gap-4 text-xs">
                  <span className="text-red-400">
                    你的答案：
                    {item.question.type === 'choice'
                      ? `${item.result.userAnswer}. ${(item.question as ChoiceQuestion).options[item.result.userAnswer as keyof typeof item.question.options]}`
                      : item.result.userAnswer.slice(0, 50) + '...'}
                  </span>
                  {item.question.type === 'choice' && (
                    <span className="text-emerald-400">
                      正确答案：{(item.question as ChoiceQuestion).answer}
                    </span>
                  )}
                </div>

                {/* Retry link */}
                <div className="mt-3 pt-3 border-t border-gray-800 flex items-center gap-3">
                  <Link
                    href={`/quiz/${item.componentId}`}
                    className="flex items-center gap-1.5 text-sm text-amber-400 hover:text-amber-300 transition-colors"
                  >
                    <RefreshCw size={14} />
                    重新练习
                  </Link>
                  <Link
                    href={`/${item.componentId}`}
                    className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-300 transition-colors"
                  >
                    <BookOpen size={14} />
                    复习教程
                  </Link>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
