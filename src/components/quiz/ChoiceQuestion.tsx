'use client';

import { useState } from 'react';
import { ChoiceQuestion as ChoiceQuestionType, QuizResult } from '@/types';
import { Check, X, ChevronDown, ChevronUp } from 'lucide-react';

interface ChoiceQuestionProps {
  question: ChoiceQuestionType;
  onResult: (result: QuizResult) => void;
  existingResult?: QuizResult;
}

export default function ChoiceQuestion({
  question,
  onResult,
  existingResult,
}: ChoiceQuestionProps) {
  const [selected, setSelected] = useState<string | null>(
    existingResult?.userAnswer ?? null
  );
  const [showExplanation, setShowExplanation] = useState(!!existingResult);

  const isAnswered = selected !== null;
  const isCorrect = selected === question.answer;

  const handleSelect = (value: string) => {
    if (selected !== null) return; // Already answered
    setSelected(value);
    setShowExplanation(true);

    const result: QuizResult = {
      userAnswer: value,
      isCorrect: value === question.answer,
      timestamp: Date.now(),
    };
    onResult(result);
  };

  const optionKeys = ['A', 'B', 'C', 'D'] as const;
  const difficultyLabel = {
    easy: '简单',
    medium: '中等',
    hard: '困难',
  };
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
        <span className="text-xs text-gray-600">选择题</span>
      </div>

      {/* Stem */}
      <div className="text-lg text-gray-100 mb-6 leading-relaxed">{question.stem}</div>

      {/* Options */}
      <div className="space-y-3 mb-6">
        {optionKeys.map(key => {
          const optionText = question.options[key];
          if (!optionText) return null;

          const isSelected = selected === key;
          const isRightAnswer = key === question.answer;
          let optionStyle = 'border-gray-700 bg-gray-900 hover:border-gray-600';

          if (isAnswered) {
            if (isRightAnswer) {
              optionStyle =
                'border-emerald-500 bg-emerald-500/10 hover:border-emerald-500';
            } else if (isSelected && !isCorrect) {
              optionStyle = 'border-red-500 bg-red-500/10 hover:border-red-500';
            } else {
              optionStyle = 'border-gray-700 bg-gray-900 opacity-60';
            }
          }

          return (
            <button
              key={key}
              onClick={() => handleSelect(key)}
              disabled={isAnswered}
              className={`w-full flex items-center gap-4 p-4 rounded-xl border text-left transition-all ${optionStyle}`}
            >
              {/* Option badge */}
              <span
                className={`flex-shrink-0 w-8 h-8 rounded-lg flex items-center justify-center text-sm font-semibold ${
                  isAnswered && isRightAnswer
                    ? 'bg-emerald-500 text-white'
                    : isSelected && !isCorrect
                      ? 'bg-red-500 text-white'
                      : 'bg-gray-800 text-gray-400'
                }`}
              >
                {key}
              </span>
              <span className="flex-1 text-gray-200 text-sm">{optionText}</span>
              {/* Result icon */}
              {isAnswered && isRightAnswer && <Check size={18} className="text-emerald-400" />}
              {isAnswered && isSelected && !isCorrect && (
                <X size={18} className="text-red-400" />
              )}
            </button>
          );
        })}
      </div>

      {/* Result banner */}
      {isAnswered && (
        <div
          className={`mb-4 p-4 rounded-xl border ${
            isCorrect
              ? 'bg-emerald-500/5 border-emerald-500/30'
              : 'bg-red-500/5 border-red-500/30'
          }`}
        >
          <div className="flex items-center gap-2 text-sm font-medium mb-1">
            {isCorrect ? (
              <>
                <Check size={16} className="text-emerald-400" />
                <span className="text-emerald-400">回答正确！</span>
              </>
            ) : (
              <>
                <X size={16} className="text-red-400" />
                <span className="text-red-400">
                  回答错误，正确答案是 {question.answer}
                </span>
              </>
            )}
          </div>
        </div>
      )}

      {/* Explanation toggle */}
      {isAnswered && (
        <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden">
          <button
            onClick={() => setShowExplanation(!showExplanation)}
            className="w-full flex items-center justify-between p-4 text-sm text-gray-400 hover:text-gray-200 transition-colors"
          >
            <span className="font-medium">📖 题目解析</span>
            {showExplanation ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
          </button>
          {showExplanation && (
            <div className="px-4 pb-4">
              <div className="border-t border-gray-800 pt-4 text-sm text-gray-400 leading-relaxed whitespace-pre-wrap">
                {question.explanation}
              </div>
            </div>
          )}
        </div>
      )}
    </div>
  );
}
