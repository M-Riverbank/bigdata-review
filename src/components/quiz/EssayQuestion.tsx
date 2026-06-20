'use client';

import { useState } from 'react';
import { EssayQuestion as EssayQuestionType, QuizResult, JudgeResponse } from '@/types';
import { Send, Check, X, Loader2, ChevronDown, ChevronUp } from 'lucide-react';

interface EssayQuestionProps {
  question: EssayQuestionType;
  onResult: (result: QuizResult) => void;
  existingResult?: QuizResult;
}

export default function EssayQuestion({
  question,
  onResult,
  existingResult,
}: EssayQuestionProps) {
  const [answer, setAnswer] = useState(existingResult?.userAnswer ?? '');
  const [judging, setJudging] = useState(false);
  const [judgeResult, setJudgeResult] = useState<JudgeResponse | null>(
    existingResult
      ? {
          score: existingResult.score ?? 0,
          feedback: existingResult.aiFeedback ?? '',
          isPass: existingResult.isCorrect,
        }
      : null
  );
  const [showSample, setShowSample] = useState(false);
  const [error, setError] = useState('');

  const isSubmitted = judgeResult !== null;

  const handleSubmit = async () => {
    if (!answer.trim() || judging) return;
    setJudging(true);
    setError('');

    try {
      const res = await fetch('/api/judge', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          stem: question.stem,
          userAnswer: answer,
          referencePoints: question.referencePoints,
        }),
      });

      if (!res.ok) {
        throw new Error(`请求失败 (${res.status})`);
      }

      const data: JudgeResponse = await res.json();
      setJudgeResult(data);

      const result: QuizResult = {
        userAnswer: answer,
        isCorrect: data.isPass,
        score: data.score,
        aiFeedback: data.feedback,
        timestamp: Date.now(),
      };
      onResult(result);
    } catch (err: any) {
      setError(err.message || 'AI 判题请求失败，请稍后重试');
    } finally {
      setJudging(false);
    }
  };

  const difficultyLabel = { easy: '简单', medium: '中等', hard: '困难' };
  const difficultyColor = {
    easy: 'bg-emerald-500/10 text-emerald-400',
    medium: 'bg-amber-500/10 text-amber-400',
    hard: 'bg-red-500/10 text-red-400',
  };

  return (
    <div>
      {/* Difficulty badge */}
      <div className="flex items-center gap-2 mb-4">
        <span
          className={`text-xs px-2 py-0.5 rounded-full ${difficultyColor[question.difficulty]}`}
        >
          {difficultyLabel[question.difficulty]}
        </span>
        <span className="text-xs text-gray-600">简答题（AI 评分）</span>
      </div>

      {/* Stem */}
      <div className="text-lg text-gray-100 mb-6 leading-relaxed">{question.stem}</div>

      {/* Answer input */}
      <div className="mb-4">
        <textarea
          value={answer}
          onChange={e => setAnswer(e.target.value)}
          disabled={isSubmitted}
          placeholder="请输入你的回答..."
          rows={6}
          className={`w-full bg-gray-900 border rounded-xl p-4 text-gray-200 text-sm leading-relaxed resize-vertical focus:outline-none focus:ring-2 focus:ring-emerald-500/50 transition-colors ${
            isSubmitted
              ? 'border-gray-700'
              : 'border-gray-700 hover:border-gray-600'
          }`}
        />
      </div>

      {/* Submit button */}
      {!isSubmitted && (
        <button
          onClick={handleSubmit}
          disabled={!answer.trim() || judging}
          className="flex items-center gap-2 px-6 py-2.5 bg-emerald-600 hover:bg-emerald-500 disabled:bg-gray-700 disabled:text-gray-500 text-white text-sm font-medium rounded-lg transition-colors"
        >
          {judging ? (
            <>
              <Loader2 size={16} className="animate-spin" />
              AI 评判中...
            </>
          ) : (
            <>
              <Send size={16} />
              提交评判
            </>
          )}
        </button>
      )}

      {/* Error */}
      {error && (
        <div className="mt-4 p-4 rounded-xl bg-red-500/5 border border-red-500/30 text-sm text-red-400">
          {error}
        </div>
      )}

      {/* Judge result */}
      {judgeResult && (
        <div className="mt-6 space-y-4">
          {/* Score banner */}
          <div
            className={`p-5 rounded-xl border ${
              judgeResult.isPass
                ? 'bg-emerald-500/5 border-emerald-500/30'
                : 'bg-amber-500/5 border-amber-500/30'
            }`}
          >
            <div className="flex items-center justify-between mb-2">
              <div className="flex items-center gap-2">
                {judgeResult.isPass ? (
                  <Check size={20} className="text-emerald-400" />
                ) : (
                  <X size={20} className="text-amber-400" />
                )}
                <span
                  className={`font-medium ${
                    judgeResult.isPass ? 'text-emerald-400' : 'text-amber-400'
                  }`}
                >
                  {judgeResult.isPass ? '通过' : '未完全通过'}
                </span>
              </div>
              <span className="text-2xl font-bold text-gray-100">
                {judgeResult.score}
                <span className="text-sm text-gray-500"> 分</span>
              </span>
            </div>
          </div>

          {/* AI Feedback */}
          <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
            <h4 className="text-sm font-medium text-gray-300 mb-2">💬 AI 点评</h4>
            <p className="text-sm text-gray-400 leading-relaxed whitespace-pre-wrap">
              {judgeResult.feedback}
            </p>
          </div>

          {/* Sample answer toggle */}
          <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
            <button
              onClick={() => setShowSample(!showSample)}
              className="w-full flex items-center justify-between p-4 text-sm text-gray-400 hover:text-gray-200 transition-colors"
            >
              <span className="font-medium">📖 参考答案</span>
              {showSample ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
            </button>
            {showSample && (
              <div className="px-4 pb-4">
                <div className="border-t border-gray-800 pt-4 text-sm text-gray-400 leading-relaxed whitespace-pre-wrap">
                  {question.sampleAnswer}
                </div>
              </div>
            )}
          </div>

          {/* Reference points */}
          {showSample && (
            <div className="bg-gray-900 border border-gray-800 rounded-xl p-5">
              <h4 className="text-sm font-medium text-gray-300 mb-2">
                📋 评分要点
              </h4>
              <ul className="space-y-1">
                {question.referencePoints.map((point, i) => (
                  <li
                    key={i}
                    className="text-sm text-gray-500 flex items-center gap-2"
                  >
                    <span className="w-1.5 h-1.5 rounded-full bg-gray-600" />
                    {point}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
