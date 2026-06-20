'use client';

import { useState, useEffect } from 'react';

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
    // Extract H2 and H3 headings and assign IDs
    const h2Regex = /^##\s+(.+)/gm;
    const h3Regex = /^###\s+(.+)/gm;
    const items: TOCItem[] = [];

    let match;
    while ((match = h2Regex.exec(content)) !== null) {
      const text = match[1];
      const id = text
        .toLowerCase()
        .replace(/[^\w一-鿿]+/g, '-')
        .replace(/(^-|-$)/g, '');
      items.push({ id, text, level: 2 });
    }

    while ((match = h3Regex.exec(content)) !== null) {
      const text = match[1];
      const id = text
        .toLowerCase()
        .replace(/[^\w一-鿿]+/g, '-')
        .replace(/(^-|-$)/g, '');
      items.push({ id, text, level: 3 });
    }

    setHeadings(items);
  }, [content]);

  // Observe scroll to highlight active heading
  useEffect(() => {
    const observer = new IntersectionObserver(
      entries => {
        entries.forEach(entry => {
          if (entry.isIntersecting) {
            setActiveId(entry.target.id);
          }
        });
      },
      { rootMargin: '-80px 0px -70% 0px' }
    );

    headings.forEach(h => {
      const el = document.getElementById(h.id);
      if (el) observer.observe(el);
    });

    return () => observer.disconnect();
  }, [headings]);

  if (headings.length === 0) return null;

  return (
    <nav className="sticky top-20">
      <h4 className="text-sm font-semibold text-gray-300 mb-3 flex items-center gap-2">
        <span className="w-1 h-4 bg-emerald-500 rounded-full" />
        目录
      </h4>
      <ul className="space-y-1 border-l border-gray-800">
        {headings.map(h => (
          <li key={h.id}>
            <a
              href={`#${h.id}`}
              className={`block py-1 text-sm transition-colors hover:text-gray-200 ${
                h.level === 3 ? 'pl-4' : 'pl-3'
              } ${
                activeId === h.id
                  ? 'text-emerald-400 border-l-2 border-emerald-400 -ml-px'
                  : 'text-gray-500 border-l-2 border-transparent -ml-px'
              }`}
              onClick={e => {
                e.preventDefault();
                const el = document.getElementById(h.id);
                if (el) {
                  el.scrollIntoView({ behavior: 'smooth', block: 'start' });
                }
              }}
            >
              {h.text}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}
