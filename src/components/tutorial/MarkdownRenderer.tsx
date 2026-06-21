'use client';

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import rehypeSlug from 'rehype-slug';
import { common, createLowlight } from 'lowlight';
import scala from 'highlight.js/lib/languages/scala';
import sql from 'highlight.js/lib/languages/sql';
import python from 'highlight.js/lib/languages/python';
import xml from 'highlight.js/lib/languages/xml';

// Register languages not in lowlight's common bundle
const lowlight = createLowlight(common);
lowlight.register('scala', scala);
lowlight.register('sql', sql);
lowlight.register('python', python);
lowlight.register('xml', xml);
// 'java', 'bash', 'json', 'javascript', 'typescript', 'css', 'markdown' etc.
// are already in the common bundle

const extraLanguages = {
  scala,
  sql,
  python,
  xml,
};

interface MarkdownRendererProps {
  content: string;
}

export default function MarkdownRenderer({ content }: MarkdownRendererProps) {
  return (
    <div className="prose-content">
      <ReactMarkdown
        remarkPlugins={[remarkGfm]}
        rehypePlugins={[[rehypeHighlight, { languages: extraLanguages }], rehypeSlug]}
      >
        {content}
      </ReactMarkdown>
    </div>
  );
}
