'use client';

import { useRef } from 'react';
import ArticleContent from './ArticleContent';
import ArticleTOC from './ArticleTOC';
import { SectionHeading } from '@/lib/content';

interface ArticleReaderProps {
  content: string;
  component: string;
  sections: SectionHeading[];
}

export default function ArticleReader({ content, component }: ArticleReaderProps) {
  const articleRef = useRef<HTMLDivElement>(null);

  return (
    <div className="flex gap-6">
      <article className="flex-1 min-w-0">
        <div className="bg-gray-900 border border-gray-800 rounded-xl p-6 md:p-8">
          <div ref={articleRef}>
            <ArticleContent content={content} component={component} />
          </div>
        </div>
      </article>

      <aside className="hidden lg:block w-44 flex-shrink-0">
        <ArticleTOC content={content} />
      </aside>
    </div>
  );
}
