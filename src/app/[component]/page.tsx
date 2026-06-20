import { notFound } from 'next/navigation';
import { getComponent, techComponents } from '@/lib/components';
import { getArticles, countArticles } from '@/lib/content';
import { countQuestions } from '@/lib/quiz';
import Link from 'next/link';
import { BookOpen, HelpCircle, Clock, ChevronRight, BarChart3 } from 'lucide-react';
import type { Metadata } from 'next';

interface Props {
  params: Promise<{ component: string }>;
}

// Generate static params for all known components
export function generateStaticParams() {
  return techComponents.map(c => ({ component: c.id }));
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { component } = await params;
  const comp = getComponent(component);
  if (!comp) return { title: '未找到' };
  return { title: `${comp.subLabel} 教程 - 大数据面试备战` };
}

export default async function ComponentPage({ params }: Props) {
  const { component } = await params;
  const comp = getComponent(component);
  if (!comp) notFound();

  const articles = getArticles(component);
  const questionCount = countQuestions(component);

  return (
    <div>
      {/* Component header */}
      <div className="mb-8">
        <div className="flex items-center gap-2 text-sm text-gray-500 mb-2">
          <span>{comp.categoryLabel}</span>
          <ChevronRight size={14} />
          <span className="text-gray-300">{comp.subLabel}</span>
        </div>
        <h1 className="text-2xl font-bold text-gray-100 mb-2">{comp.subLabel}</h1>
        <p className="text-gray-400">{comp.description}</p>

        {/* Stats */}
        <div className="flex items-center gap-5 mt-4">
          <span className="flex items-center gap-1.5 text-sm text-gray-500">
            <BookOpen size={15} />
            {articles.length} 篇教程
          </span>
          <Link
            href={`/quiz/${component}`}
            className="flex items-center gap-1.5 text-sm text-amber-400 hover:text-amber-300 transition-colors"
          >
            <HelpCircle size={15} />
            {questionCount} 道题目
          </Link>
          <span className="flex items-center gap-1.5 text-sm text-gray-500">
            <Clock size={15} />
            约 {articles.length * 10} 分钟
          </span>
        </div>
      </div>

      {/* Topics tags */}
      <div className="flex flex-wrap gap-2 mb-8">
        {comp.topics.map(topic => (
          <span
            key={topic}
            className="px-3 py-1 text-xs rounded-full bg-gray-800 text-gray-400 border border-gray-700"
          >
            {topic}
          </span>
        ))}
      </div>

      {/* Article list */}
      <div>
        <h2 className="text-lg font-semibold text-gray-200 mb-4">教程列表</h2>
        {articles.length === 0 ? (
          <div className="bg-gray-900 border border-gray-800 rounded-xl p-8 text-center text-gray-500">
            <BookOpen size={40} className="mx-auto mb-3 text-gray-700" />
            <p>暂无教程，敬请期待</p>
          </div>
        ) : (
          <div className="space-y-3">
            {articles.map((article, idx) => (
              <Link
                key={article.slug}
                href={`/${component}/${article.slug}`}
                className="block bg-gray-900 border border-gray-800 rounded-xl p-5 hover:border-gray-700 hover:bg-gray-850 transition-all group"
              >
                <div className="flex items-center gap-4">
                  {/* Order number */}
                  <div className="flex-shrink-0 w-9 h-9 rounded-lg bg-gray-800 flex items-center justify-center text-sm font-mono text-gray-500 group-hover:bg-emerald-500/10 group-hover:text-emerald-400 transition-colors">
                    {idx + 1}
                  </div>
                  <div className="flex-1 min-w-0">
                    <h3 className="text-gray-200 font-medium group-hover:text-emerald-400 transition-colors truncate">
                      {article.title}
                    </h3>
                    <div className="flex items-center gap-3 mt-1 text-xs text-gray-600">
                      <span className="flex items-center gap-1">
                        <Clock size={11} />
                        约 10 分钟
                      </span>
                      <span className="flex items-center gap-1">
                        <BarChart3 size={11} />
                        {idx === 0 ? '入门' : idx < articles.length - 1 ? '进阶' : '深入'}
                      </span>
                    </div>
                  </div>
                  <ChevronRight size={18} className="text-gray-600 group-hover:text-gray-400 transition-colors" />
                </div>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
