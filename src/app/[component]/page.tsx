import { notFound } from 'next/navigation';
import { getComponent, techComponents } from '@/lib/components';
import { getArticles, countArticles, getGroupsForComponent } from '@/lib/content';
import { countQuestions } from '@/lib/quiz';
import Link from 'next/link';
import { BookOpen, HelpCircle, Clock, ChevronRight } from 'lucide-react';
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

const difficultyStyles: Record<string, { bg: string; text: string; label: string }> = {
  '入门': { bg: 'bg-emerald-500/10', text: 'text-emerald-400', label: '入门' },
  '基础': { bg: 'bg-yellow-500/10', text: 'text-yellow-400', label: '基础' },
  '进阶': { bg: 'bg-orange-500/10', text: 'text-orange-400', label: '进阶' },
  '高阶': { bg: 'bg-red-500/10', text: 'text-red-400', label: '高阶' },
  '综合': { bg: 'bg-purple-500/10', text: 'text-purple-400', label: '综合' },
};

export default async function ComponentPage({ params }: Props) {
  const { component } = await params;
  const comp = getComponent(component);
  if (!comp) notFound();

  const articles = getArticles(component);
  const questionCount = countQuestions(component);

  // Group articles using per-component group definitions
  const componentGroups = getGroupsForComponent(component);
  const groupedArticles = componentGroups.map(g => ({
    ...g,
    items: articles.filter(a => g.articles.includes(
      String(a.order).padStart(2, '0')
    )),
  })).filter(g => g.items.length > 0);

  // Fallback: if no groups matched (e.g. other components), show flat list
  const hasGroups = groupedArticles.length > 0;

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

      {/* Article list — grouped or flat */}
      {articles.length === 0 ? (
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-8 text-center text-gray-500">
          <BookOpen size={40} className="mx-auto mb-3 text-gray-700" />
          <p>暂无教程，敬请期待</p>
        </div>
      ) : hasGroups ? (
        /* Grouped layout */
        <div className="space-y-10">
          {groupedArticles.map(group => (
            <section key={group.id}>
              {/* Group header */}
              <h2 className="text-base font-semibold text-gray-200 mb-4 flex items-center gap-2">
                <span className="w-1 h-5 bg-emerald-500 rounded-full" />
                {group.label}
                <span className="text-xs text-gray-600 font-normal ml-1">
                  {group.items.length} 篇
                </span>
              </h2>
              <div className="space-y-3">
                {group.items.map((article, idx) => {
                  const diffStyle = difficultyStyles[article.difficulty] || difficultyStyles['基础'];
                  const globalIdx = articles.findIndex(a => a.slug === article.slug) + 1;

                  return (
                    <Link
                      key={article.slug}
                      href={`/${component}/${article.slug}`}
                      className="block bg-gray-900 border border-gray-800 rounded-xl p-5 hover:border-gray-700 hover:bg-gray-850 transition-all group"
                    >
                      <div className="flex items-center gap-4">
                        {/* Order number */}
                        <div className="flex-shrink-0 w-9 h-9 rounded-lg bg-gray-800 flex items-center justify-center text-sm font-mono text-gray-500 group-hover:bg-emerald-500/10 group-hover:text-emerald-400 transition-colors">
                          {globalIdx}
                        </div>
                        <div className="flex-1 min-w-0">
                          <h3 className="text-gray-200 font-medium group-hover:text-emerald-400 transition-colors truncate">
                            {article.title.split('—').pop()?.trim() || article.title}
                          </h3>
                          <div className="flex items-center gap-3 mt-1.5">
                            <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${diffStyle.bg} ${diffStyle.text}`}>
                              {diffStyle.label}
                            </span>
                            <span className="flex items-center gap-1 text-xs text-gray-600">
                              <Clock size={11} />
                              约 {article.difficulty === '高阶' || article.difficulty === '综合' ? '15' : '10'} 分钟
                            </span>
                          </div>
                        </div>
                        <ChevronRight size={18} className="text-gray-600 group-hover:text-gray-400 transition-colors flex-shrink-0" />
                      </div>
                    </Link>
                  );
                })}
              </div>
            </section>
          ))}
        </div>
      ) : (
        /* Flat list for non-Scala components (fallback) */
        <div>
          <h2 className="text-lg font-semibold text-gray-200 mb-4">教程列表</h2>
          <div className="space-y-3">
            {articles.map((article, idx) => {
              const diffStyle = difficultyStyles[article.difficulty] || difficultyStyles['基础'];

              return (
                <Link
                  key={article.slug}
                  href={`/${component}/${article.slug}`}
                  className="block bg-gray-900 border border-gray-800 rounded-xl p-5 hover:border-gray-700 hover:bg-gray-850 transition-all group"
                >
                  <div className="flex items-center gap-4">
                    <div className="flex-shrink-0 w-9 h-9 rounded-lg bg-gray-800 flex items-center justify-center text-sm font-mono text-gray-500 group-hover:bg-emerald-500/10 group-hover:text-emerald-400 transition-colors">
                      {idx + 1}
                    </div>
                    <div className="flex-1 min-w-0">
                      <h3 className="text-gray-200 font-medium group-hover:text-emerald-400 transition-colors truncate">
                        {article.title}
                      </h3>
                      <div className="flex items-center gap-3 mt-1.5">
                        <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${diffStyle.bg} ${diffStyle.text}`}>
                          {diffStyle.label}
                        </span>
                        <span className="flex items-center gap-1 text-xs text-gray-600">
                          <Clock size={11} />
                          约 10 分钟
                        </span>
                      </div>
                    </div>
                    <ChevronRight size={18} className="text-gray-600 group-hover:text-gray-400 transition-colors flex-shrink-0" />
                  </div>
                </Link>
              );
            })}
          </div>
        </div>
      )}
    </div>
  );
}
