'use client';

import { useState, useEffect } from 'react';
import { ChevronDown, ChevronUp, Check, X, Lightbulb, ExternalLink } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

interface InterviewQuestion {
  id: string;
  type: 'choice' | 'essay';
  difficulty: 'easy' | 'medium' | 'hard';
  stem: string;
  options?: { A: string; B: string; C: string; D: string };
  answer?: string;
  explanation?: string;
  answerText?: string;
  source: string;
}

interface InterviewData {
  componentId: string;
  bindings: Record<string, string[]>;
  questions: InterviewQuestion[];
}

interface SectionHeading {
  heading: string;
  id: string;
}

interface InterviewSidebarProps {
  component: string;
  currentSectionId: string | null;
  sections: SectionHeading[];
}

const difficultyLabels: Record<string, string> = {
  easy: '简单',
  medium: '中等',
  hard: '困难',
};

const difficultyColors: Record<string, string> = {
  easy: 'bg-emerald-500/10 text-emerald-400',
  medium: 'bg-amber-500/10 text-amber-400',
  hard: 'bg-red-500/10 text-red-400',
};

export default function InterviewSidebar({ component, currentSectionId, sections }: InterviewSidebarProps) {
  const [questions, setQuestions] = useState<InterviewQuestion[]>([]);
  const [expandedAnswers, setExpandedAnswers] = useState<Set<string>>(new Set());
  const [choiceSelections, setChoiceSelections] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(false);
  const [interviewData, setInterviewData] = useState<InterviewData | null>(null);

  // Fetch interview data once
  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    setError(false);

    const tryFetch = async () => {
      let res = await fetch(`/data/${component}-interview.json`);
      if (!res.ok) {
        res = await fetch(`/data/${component}.json`);
      }
      if (!res.ok) throw new Error('No data');

      const data: InterviewData = await res.json();
      if (cancelled) return;

      if (!data.bindings) {
        data.bindings = {};
      }

      setInterviewData(data);
      setLoading(false);
    };

    tryFetch().catch(() => {
      if (!cancelled) { setError(true); setLoading(false); }
    });

    return () => { cancelled = true; };
  }, [component]);

  // Update questions when section or data changes
  useEffect(() => {
    if (!interviewData || !currentSectionId) return;

    const bindingIds = interviewData.bindings[currentSectionId];
    if (!bindingIds?.length) {
      setQuestions([]);
      return;
    }

    const bound = bindingIds
      .map(id => interviewData.questions.find(q => q.id === id))
      .filter(Boolean) as InterviewQuestion[];
    setQuestions(bound);
  }, [currentSectionId, interviewData]);

  const toggleAnswer = (id: string) => {
    setExpandedAnswers(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  };

  const handleChoice = (qid: string, key: string) => {
    if (choiceSelections[qid]) return;
    setChoiceSelections(prev => ({ ...prev, [qid]: key }));
  };

  // Loading state
  if (loading) {
    return (
      <div className="w-96 flex-shrink-0 hidden xl:block">
        <div className="sticky top-20">
          <div className="flex items-center gap-2 mb-4">
            <Lightbulb size={16} className="text-amber-400" />
            <h4 className="text-sm font-semibold text-gray-300">相关面试题</h4>
          </div>
          <div className="text-xs text-gray-600 animate-pulse">加载面试题中...</div>
        </div>
      </div>
    );
  }

  // Error — hide sidebar
  if (error) {
    return null;
  }

  return (
    <div className="w-96 flex-shrink-0 hidden xl:block"><div className="sticky top-20">
      <div className="flex items-center gap-2 mb-4">
        <Lightbulb size={16} className="text-amber-400" />
        <h4 className="text-sm font-semibold text-gray-300">相关面试题</h4>
        {questions.length > 0 && (
          <span className="text-xs text-gray-600">({questions.length})</span>
        )}
      </div>

      {questions.length === 0 ? (
        <p className="text-xs text-gray-600 leading-relaxed">
          当前知识点暂无匹配面试题。浏览更多内容或前往
          <a href={`/quiz/${component}`} className="text-amber-400 hover:text-amber-300 ml-1">
            题库
          </a>
          练习。
        </p>
      ) : (
        <div className="space-y-3 max-h-[calc(100vh-8rem)] overflow-y-auto pr-2 scrollbar-thin">
          {questions.map((q, idx) => (
            <div
              key={q.id}
              className="bg-gray-900 border border-gray-800 rounded-lg p-3.5 hover:border-gray-700 transition-colors"
            >
              {/* Question meta */}
              <div className="flex items-center gap-1.5 mb-2 flex-wrap">
                <span className="text-[10px] font-mono text-gray-600">Q{idx + 1}</span>
                <span className={`text-[10px] px-1.5 py-0.5 rounded-full ${difficultyColors[q.difficulty]}`}>
                  {difficultyLabels[q.difficulty]}
                </span>
                {q.type === 'choice' ? (
                  <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-gray-800 text-gray-500">
                    选择题
                  </span>
                ) : (
                  <span className="text-[10px] px-1.5 py-0.5 rounded-full bg-gray-800 text-gray-500">
                    问答题
                  </span>
                )}
              </div>

              {/* Stem */}
              <p className="text-sm text-gray-200 leading-relaxed mb-2.5">{q.stem}</p>

              {/* Choice question */}
              {q.type === 'choice' && q.options && (
                <div className="space-y-1 mb-2">
                  {(['A', 'B', 'C', 'D'] as const).map(key => {
                    const optText = q.options![key];
                    if (!optText) return null;

                    const selected = choiceSelections[q.id];
                    const isSelected = selected === key;
                    const isRight = key === q.answer;
                    let btnStyle = 'border-gray-700 hover:border-gray-600';

                    if (selected) {
                      if (isRight) btnStyle = 'border-emerald-500 bg-emerald-500/10';
                      else if (isSelected) btnStyle = 'border-red-500 bg-red-500/10';
                      else btnStyle = 'border-gray-700 opacity-50';
                    }

                    return (
                      <button
                        key={key}
                        onClick={() => handleChoice(q.id, key)}
                        disabled={!!selected}
                        className={`w-full flex items-start gap-2 px-2 py-1.5 rounded border text-xs text-left transition-colors ${btnStyle}`}
                      >
                        <span className={`w-5 h-5 rounded flex-shrink-0 flex items-center justify-center text-[10px] font-semibold mt-0.5 ${
                          selected && isRight
                            ? 'bg-emerald-500 text-white'
                            : selected && isSelected
                            ? 'bg-red-500 text-white'
                            : 'bg-gray-800 text-gray-400'
                        }`}>
                          {key}
                        </span>
                        <span className="flex-1 text-gray-300 whitespace-normal break-words leading-snug">{optText}</span>
                        {selected && isRight && <Check size={12} className="text-emerald-400 flex-shrink-0 mt-0.5" />}
                        {selected && isSelected && !isRight && <X size={12} className="text-red-400 flex-shrink-0 mt-0.5" />}
                      </button>
                    );
                  })}
                </div>
              )}

              {/* Choice result feedback */}
              {q.type === 'choice' && choiceSelections[q.id] && (
                <div className={`p-2 rounded-lg text-xs ${
                  choiceSelections[q.id] === q.answer
                    ? 'bg-emerald-500/5 border border-emerald-500/20 text-emerald-400'
                    : 'bg-red-500/5 border border-red-500/20 text-red-400'
                }`}>
                  {choiceSelections[q.id] === q.answer
                    ? '✅ 回答正确！'
                    : `❌ 正确答案是 ${q.answer}`
                  }
                  {q.explanation && (
                    <p className="text-gray-400 mt-1 leading-relaxed">{q.explanation}</p>
                  )}
                </div>
              )}

              {/* Essay question — toggle answer */}
              {q.type === 'essay' && (
                <div>
                  <button
                    onClick={() => toggleAnswer(q.id)}
                    className="flex items-center gap-1 text-xs text-amber-400 hover:text-amber-300 transition-colors"
                  >
                    {expandedAnswers.has(q.id)
                      ? <ChevronUp size={12} />
                      : <ChevronDown size={12} />
                    }
                    {expandedAnswers.has(q.id) ? '收起答案' : '点击查看答案'}
                  </button>

                  {expandedAnswers.has(q.id) && (
                    <div className="mt-2 p-3 bg-gray-800/50 rounded-lg border border-gray-700 text-xs text-gray-300 leading-relaxed prose-content">
                      <ReactMarkdown>
                        {q.answerText || q.answer || ''}
                      </ReactMarkdown>
                    </div>
                  )}
                </div>
              )}

              {/* Source */}
              <div className="mt-2 pt-2 border-t border-gray-800 flex items-center gap-1 text-[10px] text-gray-600">
                <ExternalLink size={9} />
                <span>{q.source}</span>
              </div>
            </div>
          ))}
        </div>
      )}
    </div></div>
  );
}
