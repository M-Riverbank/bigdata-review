'use client';

interface QuizProgressProps {
  current: number;
  total: number;
  correct: number;
  totalAnswered: number;
}

export default function QuizProgress({
  current,
  total,
  correct,
  totalAnswered,
}: QuizProgressProps) {
  const progress = total > 0 ? ((current) / total) * 100 : 0;

  return (
    <div className="mb-6">
      {/* Progress bar */}
      <div className="flex items-center justify-between text-xs text-gray-500 mb-2">
        <span>
          第 {current} / {total} 题
        </span>
        <span>
          正确 {correct} / {totalAnswered}
          {totalAnswered > 0 && ` (${Math.round((correct / totalAnswered) * 100)}%)`}
        </span>
      </div>
      <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
        <div
          className="h-full bg-emerald-500 rounded-full transition-all duration-300"
          style={{ width: `${progress}%` }}
        />
      </div>
    </div>
  );
}
