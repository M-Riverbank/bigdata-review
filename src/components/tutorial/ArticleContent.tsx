'use client';

import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import rehypeHighlight from 'rehype-highlight';
import rehypeSlug from 'rehype-slug';
import GithubSlugger from 'github-slugger';
import { useMemo } from 'react';
import { common, createLowlight } from 'lowlight';
import scala from 'highlight.js/lib/languages/scala';
import sql from 'highlight.js/lib/languages/sql';
import python from 'highlight.js/lib/languages/python';
import xml from 'highlight.js/lib/languages/xml';
import InterviewQuestionsInline from './InterviewQuestionsInline';

// Register extra languages not in lowlight's common bundle
const lowlight = createLowlight(common);
lowlight.register('scala', scala);
lowlight.register('sql', sql);
lowlight.register('python', python);
lowlight.register('xml', xml);

const extraLanguages = { scala, sql, python, xml };

interface ArticleContentProps {
  content: string;
  component: string;
}

// Split markdown content by ## sections
function splitSections(markdown: string): { heading: string; id: string; content: string }[] {
  const slugger = new GithubSlugger();
  const sections: { heading: string; id: string; content: string }[] = [];
  const lines = markdown.split('\n');
  let currentHeading = '';
  let currentContent: string[] = [];
  let firstH1 = true;

  for (const line of lines) {
    if (line.startsWith('## ') && !line.startsWith('### ')) {
      // Save previous section
      if (currentContent.length > 0 || currentHeading) {
        const slug = slugger.slug(currentHeading);
        sections.push({ heading: currentHeading, id: slug, content: currentContent.join('\n').trim() });
        slugger.reset();
      }
      currentHeading = line.replace(/^##\s+/, '').trim();
      currentContent = [];
    } else if (line.startsWith('# ') && firstH1) {
      // Title line — skip for sectioning
      firstH1 = false;
    } else {
      currentContent.push(line);
    }
  }
  // Last section
  if (currentContent.length > 0 || currentHeading) {
    const slug = slugger.slug(currentHeading);
    sections.push({ heading: currentHeading, id: slug, content: currentContent.join('\n').trim() });
  }

  return sections;
}

export default function ArticleContent({ content, component }: ArticleContentProps) {
  const sections = useMemo(() => splitSections(content), [content]);

  // Extract the H1 title and everything before first ##
  const h1Match = content.match(/^#\s+(.+)/m);
  const title = h1Match ? h1Match[1].trim() : '';
  const firstH2Index = content.search(/^##\s+/m);
  const preamble = firstH2Index > 0 ? content.substring(0, firstH2Index).trim() : '';

  return (
    <div className="prose-content">
      {/* Preamble (H1 + content before first ##) */}
      {preamble && (
        <div className="mb-6">
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            rehypePlugins={[[rehypeHighlight, { languages: extraLanguages }], rehypeSlug]}
          >
            {preamble}
          </ReactMarkdown>
        </div>
      )}

      {/* Sections */}
      {sections.map((section) => (
        <div
          key={section.id}
          className="mb-8"
          id={`section-${section.id}`}
          data-section-id={section.id}
        >
          {/* Section heading */}
          <h2
            className="text-xl font-semibold text-gray-200 mt-8 mb-4 pl-3 border-l-4 border-emerald-500/50 scroll-mt-20"
            id={section.id}
          >
            {section.heading}
          </h2>

          {/* Section content — single flow for all components */}
          <ReactMarkdown
            remarkPlugins={[remarkGfm]}
            rehypePlugins={[[rehypeHighlight, { languages: extraLanguages }], rehypeSlug]}
          >
            {section.content}
          </ReactMarkdown>

          {/* Inline interview questions after each section */}
          <InterviewQuestionsInline component={component} sectionId={section.id} />
        </div>
      ))}
    </div>
  );
}
