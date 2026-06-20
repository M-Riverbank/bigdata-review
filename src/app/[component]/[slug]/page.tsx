import { notFound } from 'next/navigation';
import { getComponent, techComponents } from '@/lib/components';
import { getArticleContent, getArticles, getAdjacentArticles } from '@/lib/content';
import MarkdownRenderer from '@/components/tutorial/MarkdownRenderer';
import ArticleTOC from '@/components/tutorial/ArticleTOC';
import Link from 'next/link';
import { ChevronLeft, ChevronRight, ArrowLeft } from 'lucide-react';
import type { Metadata } from 'next';

interface Props {
  params: Promise<{ component: string; slug: string }>;
}

export async function generateMetadata({ params }: Props): Promise<Metadata> {
  const { component, slug } = await params;
  const articles = getArticles(component);
  const article = articles.find(a => a.slug === slug);
  return { title: article ? `${article.title} - 大数据面试备战` : '未找到' };
}

// Generate static params
export function generateStaticParams() {
  const params: { component: string; slug: string }[] = [];
  techComponents.forEach(c => {
    const articles = getArticles(c.id);
    articles.forEach(a => {
      params.push({ component: c.id, slug: a.slug });
    });
  });
  return params;
}

export default async function ArticlePage({ params }: Props) {
  const { component, slug } = await params;
  const comp = getComponent(component);
  if (!comp) notFound();

  const content = getArticleContent(component, slug);
  if (!content) notFound();

  const articles = getArticles(component);
  const currentIdx = articles.findIndex(a => a.slug === slug);
  const { prev, next } = getAdjacentArticles(component, slug);

  return (
    <div className="max-w-6xl mx-auto">
      {/* Back link */}
      <Link
        href={`/${component}`}
        className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-300 mb-6 transition-colors"
      >
        <ArrowLeft size={15} />
        返回 {comp.subLabel} 教程列表
      </Link>

      <div className="flex gap-10">
        {/* Main content */}
        <article className="flex-1 min-w-0">
          <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 md:p-8">
            <MarkdownRenderer content={content} />
          </div>

          {/* Progress marker for LocalStorage — client component */}
          <ArticleProgressMarker component={component} slug={slug} />

          {/* Prev / Next */}
          <div className="flex items-center justify-between mt-8">
            {prev ? (
              <Link
                href={`/${component}/${prev.slug}`}
                className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-300 transition-colors group"
              >
                <ChevronLeft size={16} className="group-hover:-translate-x-0.5 transition-transform" />
                <div>
                  <div className="text-xs text-gray-600">上一篇</div>
                  <div>{prev.title}</div>
                </div>
              </Link>
            ) : (
              <div />
            )}

            {next ? (
              <Link
                href={`/${component}/${next.slug}`}
                className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-300 transition-colors text-right group"
              >
                <div>
                  <div className="text-xs text-gray-600">下一篇</div>
                  <div>{next.title}</div>
                </div>
                <ChevronRight size={16} className="group-hover:translate-x-0.5 transition-transform" />
              </Link>
            ) : (
              <div />
            )}
          </div>
        </article>

        {/* TOC sidebar */}
        <aside className="hidden lg:block w-56 flex-shrink-0">
          <ArticleTOC content={content} />
        </aside>
      </div>
    </div>
  );
}

// Client marker component — marks article as read
function ArticleProgressMarker({ component, slug }: { component: string; slug: string }) {
  // This is a server component rendered inside a client-page boundary
  // We use a script tag approach
  const key = `${component}/${slug}`;

  return (
    <script
      dangerouslySetInnerHTML={{
        __html: `
          (function() {
            try {
              var KEY = 'bigdata-review-progress';
              var data = JSON.parse(localStorage.getItem(KEY) || '{}');
              if (!data.completedArticles) data.completedArticles = {};
              data.completedArticles['${key}'] = true;
              localStorage.setItem(KEY, JSON.stringify(data));
            } catch(e) {}
          })();
        `,
      }}
    />
  );
}
