'use client';

import { useState, useEffect, useMemo } from 'react';
import GithubSlugger from 'github-slugger';

interface TOCItem {
  id: string;
  text: string;
  level: number;
}

interface ArticleTOCProps {
  content: string;
}

export default function ArticleTOC({ content }: ArticleTOCProps) {
  const [activeId, setActiveId] = useState('');
  const [headings, setHeadings] = useState<TOCItem[]>([]);

  useEffect(() => {
    // Use github-slugger (same as rehype-slug) for consistent IDs
    const slugger = new GithubSlugger();
    const h2Regex = /^##\s+(.+)/gm;
    const h3Regex = /^###\s+(.+)/gm;
    const items: TOCItem[] = [];

    let match;
    while ((match = h2Regex.exec(content)) !== null) {
      const text = match[1].trim();
      const id = slugger.slug(text);
      items.push({ id, text, level: 2 });
    }

    while ((match = h3Regex.exec(content)) !== null) {
      const text = match[1].trim();
      const id = slugger.slug(text);
      items.push({ id, text, level: 3 });
    }

    setHeadings(items);
  }, [content]);

  // Scroll-spy: observe which heading is in view
  useEffect(() => {
    if (headings.length === 0) return;

    const observer = new IntersectionObserver(
      entries => {
        const visible = entries.filter(e => e.isIntersecting);
        if (visible.length > 0) {
          setActiveId(visible[0].target.id);
        }
      },
      { rootMargin: '-100px 0px -60% 0px' }
    );

    headings.forEach(h => {
      const el = document.getElementById(h.id);
      if (el) observer.observe(el);
    });

    return () => observer.disconnect();
  }, [headings]);

  // Group H3s under their preceding H2 for visual hierarchy
  const groupedHeadings = useMemo(() => {
    const result: { h2: TOCItem; h3s: TOCItem[] }[] = [];
    for (const h of headings) {
      if (h.level === 2) {
        result.push({ h2: h, h3s: [] });
      } else if (result.length > 0) {
        result[result.length - 1].h3s.push(h);
      }
    }
    return result;
  }, [headings]);

  if (headings.length === 0) return null;

  return (
    <nav className="sticky top-20">
      <h4 className="text-sm font-semibold text-gray-300 mb-4 flex items-center gap-2">
        <span className="w-1 h-4 bg-emerald-500 rounded-full" />
        目录
      </h4>
      <ul className="border-l border-gray-800">
        {groupedHeadings.map(({ h2, h3s }) => (
          <li key={h2.id} className="mb-1">
            {/* H2 item */}
            <a
              href={`#${h2.id}`}
              className={`block py-1.5 text-[13px] leading-snug transition-colors pl-3 border-l-2 -ml-px ${
                activeId === h2.id
                  ? 'text-emerald-400 border-emerald-400 font-medium'
                  : 'text-gray-400 border-transparent hover:text-gray-200'
              }`}
              onClick={e => {
                e.preventDefault();
                const el = document.getElementById(h2.id);
                if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
              }}
            >
              {h2.text}
            </a>
            {/* H3 items under this H2 */}
            {h3s.length > 0 && (
              <ul>
                {h3s.map(h3 => (
                  <li key={h3.id}>
                    <a
                      href={`#${h3.id}`}
                      className={`block py-1 text-[12px] leading-snug transition-colors pl-7 border-l-2 -ml-px ${
                        activeId === h3.id
                          ? 'text-emerald-400 border-emerald-400 font-medium'
                          : 'text-gray-500 border-transparent hover:text-gray-300'
                      }`}
                      onClick={e => {
                        e.preventDefault();
                        const el = document.getElementById(h3.id);
                        if (el) el.scrollIntoView({ behavior: 'smooth', block: 'start' });
                      }}
                    >
                      {h3.text}
                    </a>
                  </li>
                ))}
              </ul>
            )}
          </li>
        ))}
      </ul>
    </nav>
  );
}
