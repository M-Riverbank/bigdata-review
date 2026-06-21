'use client';

import { useState, useEffect } from 'react';
import { ChevronDown, ChevronUp, Check, X, Lightbulb, ExternalLink, BookOpen } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

interface InterviewQuestion {
  id: string;
  type: 'choice' | 'essay';
  difficulty: 'easy' | 'medium' | 'hard';
  stem: string;
  options?: { A: string; B: string; C: string; D: string };
  answer?: string;
  explanation?: string;
  sampleAnswer?: string;
  source: string;
}

interface InterviewData {
  componentId: string;
  bindings: Record<string, string[]>;
  questions: InterviewQuestion[];
}

interface Props {
  component: string;
  sectionId: string;
}

const difficultyColors: Record<string, string> = {
  easy: 'bg-emerald-500/10 text-emerald-400',
  medium: 'bg-amber-500/10 text-amber-400',
  hard: 'bg-red-500/10 text-red-400',
};

const difficultyLabels: Record<string, string> = {
  easy: '简单', medium: '中等', hard: '困难',
};

export default function InterviewQuestionsInline({ component, sectionId }: Props) {
  const [questions, setQuestions] = useState<InterviewQuestion[]>([]);
  const [expandedAnswers, setExpandedAnswers] = useState<Set<string>>(new Set());
  const [choiceSelections, setChoiceSelections] = useState<Record<string, string>>({});
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch(`/data/${component}-interview.json`)
      .then(res => res.ok ? res.json() : Promise.reject('no data'))
      .then((data: InterviewData) => {
        const bindingIds = data.bindings[sectionId];
        if (!bindingIds?.length) { setQuestions([]); setLoading(false); return; }
        setQuestions(bindingIds.map(id => data.questions.find(q => q.id === id)).filter(Boolean) as InterviewQuestion[]);
        setLoading(false);
      })
      .catch(() => { setQuestions([]); setLoading(false); });
  }, [component, sectionId]);

  const toggleAnswer = (id: string) => {
    setExpandedAnswers(prev => { const n = new Set(prev); n.has(id) ? n.delete(id) : n.add(id); return n; });
  };

  const handleChoice = (qid: string, key: string) => {
    if (choiceSelections[qid]) return;
    setChoiceSelections(prev => ({ ...prev, [qid]: key }));
  };

  if (loading) {
    return <div className="text-xs text-gray-700 animate-pulse pt-8">加载面试题...</div>;
  }
  if (questions.length === 0) return null;

  return (
    <div className="mt-10 mb-4">
      {/* 醒目分隔线 */}
      <div className="relative flex items-center gap-3 mb-5">
        <div className="flex-1 h-px bg-gradient-to-r from-transparent via-amber-500/40 to-transparent" />
        <div className="flex items-center gap-2 px-3 py-1 bg-amber-500/10 border border-amber-500/20 rounded-full">
          <BookOpen size={14} className="text-amber-400" />
          <span className="text-xs font-semibold text-amber-400 tracking-wide">面 试 真 题</span>
          <span className="text-[10px] text-amber-500/70">({questions.length}题)</span>
        </div>
        <div className="flex-1 h-px bg-gradient-to-r from-transparent via-amber-500/40 to-transparent" />
      </div>

      <div className="space-y-3">
        {questions.map((q, qi) => (
          <div key={q.id} className="bg-gray-900 border border-gray-700/60 rounded-lg p-4 hover:border-gray-600/80 transition-colors">
            {/* 题目标题行 */}
            <div className="flex items-start gap-2 mb-2">
              <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-amber-500/15 text-amber-400 text-[11px] font-bold flex-shrink-0 mt-0.5">{qi + 1}</span>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 mb-1 flex-wrap">
                  <span className={`text-[10px] px-1.5 py-0.5 rounded font-medium ${difficultyColors[q.difficulty]}`}>{difficultyLabels[q.difficulty]}</span>
                  {q.type === 'choice' && <span className="text-[10px] text-blue-400 bg-blue-500/10 px-1.5 py-0.5 rounded">选择题</span>}
                  {q.type === 'essay' && <span className="text-[10px] text-purple-400 bg-purple-500/10 px-1.5 py-0.5 rounded">问答题</span>}
                </div>
                <p className="text-sm text-gray-200 leading-relaxed font-medium">{q.stem}</p>
              </div>
            </div>

            {/* 选择题答案区 */}
            {q.type === 'choice' && q.options && (
              <div className="ml-7 space-y-1.5 mb-2">
                {(['A','B','C','D'] as const).map(key => {
                  const txt = q.options![key]; if (!txt) return null;
                  const sel = choiceSelections[q.id]; const isSel = sel === key; const isRight = key === q.answer;
                  let style = 'border-gray-600/50 hover:border-gray-500';
                  if (sel) {
                    if (isRight) style = 'border-emerald-500 bg-emerald-500/10';
                    else if (isSel) style = 'border-red-500 bg-red-500/10';
                    else style = 'border-gray-700/50 opacity-40';
                  }
                  return (
                    <button key={key} onClick={() => handleChoice(q.id, key)} disabled={!!sel}
                      className={`w-full flex items-start gap-2 px-3 py-2 rounded-lg border text-sm text-left transition-all ${style}`}>
                      <span className={`w-5 h-5 rounded-full flex-shrink-0 flex items-center justify-center text-[11px] font-bold mt-0.5 ${sel && isRight ? 'bg-emerald-500 text-white' : sel && isSel ? 'bg-red-500 text-white' : 'bg-gray-800 text-gray-400'}`}>{key}</span>
                      <span className="flex-1 text-gray-300 leading-relaxed">{txt}</span>
                      {sel && isRight && <Check size={14} className="text-emerald-400 flex-shrink-0 mt-0.5" />}
                      {sel && isSel && !isRight && <X size={14} className="text-red-400 flex-shrink-0 mt-0.5" />}
                    </button>
                  );
                })}
              </div>
            )}
            {q.type === 'choice' && choiceSelections[q.id] && (
              <div className={`ml-7 text-xs p-2 rounded ${choiceSelections[q.id] === q.answer ? 'bg-emerald-500/10 border border-emerald-500/20 text-emerald-300' : 'bg-red-500/10 border border-red-500/20 text-red-300'}`}>
                {choiceSelections[q.id] === q.answer ? '✅ 回答正确！' : `❌ 正确答案：${q.answer}`}
                {q.explanation && <p className="text-gray-400 mt-1 leading-relaxed">{q.explanation}</p>}
              </div>
            )}

            {/* 问答题展开区 */}
            {q.type === 'essay' && (
              <div className="ml-7">
                <button onClick={() => toggleAnswer(q.id)} className="flex items-center gap-1.5 text-xs text-amber-400 hover:text-amber-300 transition-colors font-medium">
                  {expandedAnswers.has(q.id) ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
                  {expandedAnswers.has(q.id) ? '收起答案' : '查看参考答案'}
                </button>
                {expandedAnswers.has(q.id) && (
                  <div className="mt-2 p-3 bg-gray-800/60 border border-gray-700/60 rounded-lg text-xs text-gray-300 leading-relaxed prose-content prose-sm">
                    <ReactMarkdown>{(q.sampleAnswer || q.answer || '')}</ReactMarkdown>
                  </div>
                )}
              </div>
            )}

            {/* 来源 */}
            <div className="mt-2 pt-2 border-t border-gray-800 flex items-center gap-1.5 text-[10px] text-gray-600">
              <ExternalLink size={9} />
              <span>{q.source}</span>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
