import { notFound } from 'next/navigation';
import { getComponent, techComponents } from '@/lib/components';
import { getArticleContent, getArticles, getAdjacentArticles } from '@/lib/content';
import ArticleReader from '@/components/tutorial/ArticleReader';
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

export function generateStaticParams() {
  const params: { component: string; slug: string }[] = [];
  techComponents.forEach(c => {
    const articles = getArticles(c.id);
    articles.forEach(a => params.push({ component: c.id, slug: a.slug }));
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
  const { prev, next } = getAdjacentArticles(component, slug);

  return (
    <div className="max-w-[120rem] mx-auto">
      <Link
        href={`/${component}`}
        className="inline-flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-300 mb-6 transition-colors"
      >
        <ArrowLeft size={15} />
        返回 {comp.subLabel} 教程列表
      </Link>

      <ArticleReader content={content} component={component} />

      <ArticleProgressMarker component={component} slug={slug} />

      {/* Prev / Next */}
      <div className="flex items-center justify-between mt-8">
        {prev ? (
          <Link href={`/${component}/${prev.slug}`} className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-300 transition-colors group">
            <ChevronLeft size={16} className="group-hover:-translate-x-0.5 transition-transform" />
            <div>
              <div className="text-xs text-gray-600">上一篇</div>
              <div>{prev.title}</div>
            </div>
          </Link>
        ) : <div />}
        {next ? (
          <Link href={`/${component}/${next.slug}`} className="flex items-center gap-2 text-sm text-gray-500 hover:text-gray-300 transition-colors text-right group">
            <div>
              <div className="text-xs text-gray-600">下一篇</div>
              <div>{next.title}</div>
            </div>
            <ChevronRight size={16} className="group-hover:translate-x-0.5 transition-transform" />
          </Link>
        ) : <div />}
      </div>
    </div>
  );
}

function ArticleProgressMarker({ component, slug }: { component: string; slug: string }) {
  const key = `${component}/${slug}`;
  return (
    <script dangerouslySetInnerHTML={{ __html: `
      (function() {
        try {
          var KEY = 'bigdata-review-progress';
          var data = JSON.parse(localStorage.getItem(KEY) || '{}');
          if (!data.completedArticles) data.completedArticles = {};
          data.completedArticles['${key}'] = true;
          localStorage.setItem(KEY, JSON.stringify(data));
        } catch(e) {}
      })();
    `}} />
  );
}
