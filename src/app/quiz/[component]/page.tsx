'use client';

import { useState, useEffect, use } from 'react';
import { useRouter } from 'next/navigation';
import { getComponent } from '@/lib/components';
import { ChoiceQuestion as ChoiceQuestionType, EssayQuestion as EssayQuestionType, Question, QuizResult, UserProgress } from '@/types';
import ChoiceQuestionComp from '@/components/quiz/ChoiceQuestion';
import EssayQuestionComp from '@/components/quiz/EssayQuestion';
import QuizProgress from '@/components/quiz/QuizProgress';
import { ArrowLeft, ChevronLeft, ChevronRight, Shuffle, ListOrdered, RefreshCw } from 'lucide-react';
import Link from 'next/link';

function loadProgress(): UserProgress {
  try {
    const raw = localStorage.getItem('bigdata-review-progress');
    if (raw) {
      const data = JSON.parse(raw);
      if (!data.completedArticles) data.completedArticles = {};
      if (!data.quizResults) data.quizResults = {};
      if (!data.wrongQuestionIds) data.wrongQuestionIds = [];
      return data;
    }
  } catch {}
  return { completedArticles: {}, quizResults: {}, wrongQuestionIds: [] };
}

function saveProgress(progress: UserProgress) {
  localStorage.setItem('bigdata-review-progress', JSON.stringify(progress));
}

const BATCH_SIZE = 5;

export default function QuizPage({ params }: { params: Promise<{ component: string }> }) {
  const { component } = use(params);
  const router = useRouter();
  const comp = getComponent(component);

  const [allQuestions, setAllQuestions] = useState<Question[]>([]);
  const [batchOffset, setBatchOffset] = useState(0);
  const [currentIdx, setCurrentIdx] = useState(0);
  const [results, setResults] = useState<Record<string, QuizResult>>({});
  const [mode, setMode] = useState<'sequential' | 'random'>('random');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');

  // Visible batch
  const displayQuestions = mode === 'random'
    ? allQuestions.slice(batchOffset, batchOffset + BATCH_SIZE)
    : allQuestions;

  const batchAllAnswered = mode === 'random' && displayQuestions.length > 0 &&
    displayQuestions.every(q => results[q.id]);

  const hasMoreBatches = mode === 'random' && batchOffset + BATCH_SIZE < allQuestions.length;

  useEffect(() => {
    fetch(`/data/${component}.json`)
      .then(res => {
        if (!res.ok) throw new Error('题库加载失败');
        return res.json();
      })
      .then(data => {
        let qs = data.questions as Question[];
        const progress = loadProgress();
        const savedResults: Record<string, QuizResult> = {};
        qs.forEach(q => {
          if (progress.quizResults[q.id]) {
            savedResults[q.id] = progress.quizResults[q.id];
          }
        });
        setResults(savedResults);

        if (mode === 'random') qs = shuffleArray([...qs]);
        setAllQuestions(qs);
        setBatchOffset(0);
        setCurrentIdx(0);
        setLoading(false);
      })
      .catch(err => {
        setError(err.message);
        setLoading(false);
      });
  }, [component, mode]);

  const nextBatch = () => {
    const remaining = allQuestions.slice(batchOffset + BATCH_SIZE);
    const reshuffled = shuffleArray([...remaining]);
    setAllQuestions(reshuffled);
    setBatchOffset(0);
    setCurrentIdx(0);
  };

  const handleResult = (result: QuizResult) => {
    const q = displayQuestions[currentIdx];
    if (!q) return;
    const newResults = { ...results, [q.id]: result };
    setResults(newResults);

    const progress = loadProgress();
    progress.quizResults[q.id] = result;

    if (!result.isCorrect) {
      if (!progress.wrongQuestionIds.includes(q.id)) {
        progress.wrongQuestionIds.push(q.id);
      }
    } else {
      progress.wrongQuestionIds = progress.wrongQuestionIds.filter(id => id !== q.id);
    }

    saveProgress(progress);
  };

  const goNext = () => {
    if (currentIdx < displayQuestions.length - 1) {
      setCurrentIdx(currentIdx + 1);
    }
  };

  const goPrev = () => {
    if (currentIdx > 0) {
      setCurrentIdx(currentIdx - 1);
    }
  };

  const toggleMode = () => {
    if (mode === 'sequential') {
      setMode('random');
    } else {
      setMode('sequential');
      fetch(`/data/${component}.json`)
        .then(res => res.json())
        .then(data => {
          setAllQuestions(data.questions);
          setBatchOffset(0);
          setCurrentIdx(0);
        });
    }
  };

  const totalAnswered = Object.keys(results).length;
  const totalCorrect = Object.values(results).filter(r => r.isCorrect).length;

  if (loading) {
    return (
      <div className="flex items-center justify-center h-64">
        <div className="text-gray-500">加载题库中...</div>
      </div>
    );
  }

  if (error || !comp) {
    return (
      <div className="text-center py-16">
        <p className="text-gray-500 mb-4">{error || '组件不存在'}</p>
        <Link href="/quiz" className="text-emerald-400 hover:text-emerald-300 text-sm">
          ← 返回题库总览
        </Link>
      </div>
    );
  }

  if (allQuestions.length === 0) {
    return (
      <div className="text-center py-16">
        <p className="text-gray-500 mb-4">该组件暂无题目</p>
        <Link href="/quiz" className="text-emerald-400 hover:text-emerald-300 text-sm">
          ← 返回题库总览
        </Link>
      </div>
    );
  }

  if (displayQuestions.length === 0) {
    return (
      <div className="text-center py-16">
        <p className="text-gray-500 mb-4">已无更多题目</p>
        <Link href={`/quiz/${component}`} className="text-emerald-400 hover:text-emerald-300 text-sm">
          ← 重新开始
        </Link>
      </div>
    );
  }

  const current = displayQuestions[currentIdx];

  return (
    <div className="max-w-3xl mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <Link
            href="/quiz"
            className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-300 mb-2 transition-colors"
          >
            <ArrowLeft size={15} />
            题库总览
          </Link>
          <h1 className="text-xl font-bold text-gray-100">{comp.subLabel} · 答题</h1>
        </div>
        <button
          onClick={toggleMode}
          className="flex items-center gap-2 px-4 py-2 rounded-lg border border-gray-700 text-sm text-gray-400 hover:text-gray-200 hover:border-gray-600 transition-colors"
        >
          {mode === 'sequential' ? (
            <>
              <Shuffle size={15} />
              随机出题
            </>
          ) : (
            <>
              <ListOrdered size={15} />
              顺序出题
            </>
          )}
        </button>
      </div>

      {/* Batch indicator */}
      {mode === 'random' && (
        <div className="text-xs text-gray-500 mb-3">
          第 {Math.floor(batchOffset / BATCH_SIZE) + 1} 组 · 共 {allQuestions.length} 题
          （每次 {BATCH_SIZE} 题）
        </div>
      )}

      {/* Progress */}
      <QuizProgress
        current={currentIdx + 1}
        total={displayQuestions.length}
        correct={totalCorrect}
        totalAnswered={totalAnswered}
      />

      {/* Question */}
      <div className="bg-gray-900 border border-gray-800 rounded-xl p-6">
        {current.type === 'choice' ? (
          <ChoiceQuestionComp
            key={current.id}
            question={current as ChoiceQuestionType}
            onResult={handleResult}
            existingResult={results?.[current?.id]}
          />
        ) : (
          <EssayQuestionComp
            key={current.id}
            question={current as EssayQuestionType}
            onResult={handleResult}
            existingResult={results?.[current?.id]}
            useAI={component === 'spark-sql'}
          />
        )}
      </div>

      {/* Navigation */}
      <div className="flex items-center justify-between mt-6">
        <button
          onClick={goPrev}
          disabled={currentIdx === 0}
          className="flex items-center gap-1.5 px-4 py-2 rounded-lg border border-gray-700 text-sm text-gray-400 hover:text-gray-200 hover:border-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
        >
          <ChevronLeft size={16} />
          上一题
        </button>

        <div className="flex gap-2">
          {displayQuestions.map((q, i) => {
            const r = results[q.id];
            let dotStyle = 'bg-gray-800';
            if (r) {
              dotStyle = r.isCorrect
                ? 'bg-emerald-500'
                : 'bg-red-500';
            }
            if (i === currentIdx) {
              dotStyle += ' ring-2 ring-emerald-400/50';
            }
            return (
              <button
                key={q.id}
                onClick={() => setCurrentIdx(i)}
                className={`w-3 h-3 rounded-full transition-all ${dotStyle}`}
              />
            );
          })}
        </div>

        <button
          onClick={goNext}
          disabled={currentIdx === displayQuestions.length - 1}
          className="flex items-center gap-1.5 px-4 py-2 rounded-lg border border-gray-700 text-sm text-gray-400 hover:text-gray-200 hover:border-gray-600 disabled:opacity-30 disabled:cursor-not-allowed transition-colors"
        >
          下一题
          <ChevronRight size={16} />
        </button>
      </div>

      {/* Batch completion — show "next batch" button */}
      {batchAllAnswered && (
        <div className="mt-6 text-center">
          {hasMoreBatches ? (
            <button
              onClick={nextBatch}
              className="inline-flex items-center gap-2 px-6 py-3 bg-amber-600 hover:bg-amber-500 text-white rounded-xl text-sm font-medium transition-colors"
            >
              <RefreshCw size={16} />
              下一组（{Math.min(BATCH_SIZE, allQuestions.length - batchOffset - BATCH_SIZE)} 题）
            </button>
          ) : (
            <div className="mt-8 bg-gray-900 border border-emerald-500/20 rounded-xl p-6 text-center">
              <h3 className="text-lg font-semibold text-gray-100 mb-2">🎉 全部完成！</h3>
              <p className="text-gray-400 mb-4">
                正确率：{Math.round((totalCorrect / allQuestions.length) * 100)}%
                （{totalCorrect} / {allQuestions.length}）
              </p>
              <div className="flex items-center justify-center gap-4">
                <Link
                  href={`/quiz/${component}`}
                  onClick={() => {
                    setCurrentIdx(0);
                    setResults({});
                  }}
                  className="px-4 py-2 bg-gray-800 text-gray-300 rounded-lg text-sm hover:bg-gray-700 transition-colors"
                >
                  重新答题
                </Link>
                <Link
                  href="/wrong"
                  className="px-4 py-2 bg-amber-600 text-white rounded-lg text-sm hover:bg-amber-500 transition-colors"
                >
                  查看错题
                </Link>
              </div>
            </div>
          )}
        </div>
      )}

      {/* Summary at end (sequential mode) */}
      {mode === 'sequential' && totalAnswered === allQuestions.length && (
        <div className="mt-8 bg-gray-900 border border-emerald-500/20 rounded-xl p-6 text-center">
          <h3 className="text-lg font-semibold text-gray-100 mb-2">🎉 全部完成！</h3>
          <p className="text-gray-400 mb-4">
            正确率：{Math.round((totalCorrect / allQuestions.length) * 100)}%
            （{totalCorrect} / {allQuestions.length}）
          </p>
          <div className="flex items-center justify-center gap-4">
            <Link
              href={`/quiz/${component}`}
              onClick={() => {
                setCurrentIdx(0);
                setResults({});
              }}
              className="px-4 py-2 bg-gray-800 text-gray-300 rounded-lg text-sm hover:bg-gray-700 transition-colors"
            >
              重新答题
            </Link>
            <Link
              href="/wrong"
              className="px-4 py-2 bg-amber-600 text-white rounded-lg text-sm hover:bg-amber-500 transition-colors"
            >
              查看错题
            </Link>
          </div>
        </div>
      )}
    </div>
  );
}

function shuffleArray<T>(arr: T[]): T[] {
  for (let i = arr.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [arr[i], arr[j]] = [arr[j], arr[i]];
  }
  return arr;
}
