import 'server-only';
import { Article } from '@/types';
import { ARTICLE_DIFFICULTY, getGroupsForComponent } from '@/lib/content-constants';
import fs from 'fs';
import path from 'path';
import GithubSlugger from 'github-slugger';

const CONTENT_ROOT = path.join(process.cwd(), 'content');

// Look up group info for an article by its component and order prefix
function getGroupInfo(component: string, orderPrefix: string): { group: string; groupLabel: string } {
  const groups = getGroupsForComponent(component);
  for (const g of groups) {
    if (g.articles.includes(orderPrefix)) {
      return { group: g.id, groupLabel: g.label };
    }
  }
  return { group: 'default', groupLabel: '教程' };
}

// Map component id to content directory path
// Sub-components like 'spark-sql' live under 'spark/sql/'
function getContentDir(componentId: string): string {
  const parts = componentId.split('-');
  if (parts.length >= 2 && !['hdfs', 'yarn'].includes(componentId)) {
    return path.join(CONTENT_ROOT, parts[0], parts[1]);
  }
  if (componentId === 'hdfs') return path.join(CONTENT_ROOT, 'hadoop', 'hdfs');
  if (componentId === 'yarn') return path.join(CONTENT_ROOT, 'hadoop', 'yarn');
  return path.join(CONTENT_ROOT, componentId);
}

// Get all component directory paths relative to CONTENT_ROOT
function getAllContentDirs(): { relPath: string; absPath: string }[] {
  if (!fs.existsSync(CONTENT_ROOT)) return [];

  const results: { relPath: string; absPath: string }[] = [];

  function scan(dir: string, rel: string) {
    const entries = fs.readdirSync(dir, { withFileTypes: true });
    const hasMd = entries.some(e => e.isFile() && e.name.endsWith('.md'));
    if (hasMd) {
      results.push({ relPath: rel, absPath: dir });
    }
    for (const entry of entries) {
      if (entry.isDirectory()) {
        scan(path.join(dir, entry.name), rel ? `${rel}/${entry.name}` : entry.name);
      }
    }
  }

  scan(CONTENT_ROOT, '');
  return results;
}

// Map a relative content directory to component ID
function dirToComponentId(relPath: string): string {
  return relPath.replace(/\//g, '-');
}

// Get all articles for a component
export function getArticles(component: string): Article[] {
  const dir = getContentDir(component);
  if (!fs.existsSync(dir)) return [];

  return fs
    .readdirSync(dir)
    .filter(f => f.endsWith('.md'))
    .map(f => {
      const match = f.match(/^(\d+)-(.+)\.md$/);
      const order = match ? parseInt(match[1], 10) : 999;
      const orderPrefix = match ? match[1] : '00';
      const title = match
        ? match[2].replace(/-/g, ' ').replace(/^\S/, s => s.toUpperCase())
        : f.replace(/\.md$/, '');
      let displayTitle = title;
      try {
        const raw = fs.readFileSync(path.join(dir, f), 'utf-8');
        const h1Match = raw.match(/^#\s+(.+)/m);
        if (h1Match) displayTitle = h1Match[1].trim();
      } catch (_) { /* keep filename-derived title */ }
      const difficulty = ARTICLE_DIFFICULTY[orderPrefix] || '基础';
      const groupInfo = getGroupInfo(component, orderPrefix);
      return {
        slug: f.replace(/\.md$/, ''),
        title: displayTitle,
        component,
        order,
        difficulty,
        group: groupInfo.group,
        groupLabel: groupInfo.groupLabel,
      };
    })
    .sort((a, b) => a.order - b.order);
}

// Get article content (raw Markdown string)
export function getArticleContent(component: string, slug: string): string | null {
  const dir = getContentDir(component);
  const filePath = path.join(dir, `${slug}.md`);
  if (!fs.existsSync(filePath)) return null;
  return fs.readFileSync(filePath, 'utf-8');
}

// Get all articles across all components
export function getAllArticles(): Article[] {
  const dirs = getAllContentDirs();
  return dirs.flatMap(d => {
    const component = dirToComponentId(d.relPath);
    return getArticles(component);
  });
}

// Count articles per component
export function countArticles(component: string): number {
  return getArticles(component).length;
}

// Get adjacent articles for prev/next navigation
export function getAdjacentArticles(
  component: string,
  slug: string
): { prev: Article | null; next: Article | null } {
  const articles = getArticles(component);
  const idx = articles.findIndex(a => a.slug === slug);
  return {
    prev: idx > 0 ? articles[idx - 1] : null,
    next: idx < articles.length - 1 ? articles[idx + 1] : null,
  };
}

// Extract section headings from markdown content for the interview sidebar
export interface SectionHeading {
  heading: string;
  id: string;
}

export function extractSectionHeadings(content: string): SectionHeading[] {
  const slugger = new GithubSlugger();
  const headings: SectionHeading[] = [];
  const lines = content.split('\n');
  for (const line of lines) {
    if (line.startsWith('## ') && !line.startsWith('### ')) {
      const heading = line.replace(/^##\s+/, '').trim();
      const id = slugger.slug(heading);
      slugger.reset();
      headings.push({ heading, id });
    }
  }
  return headings;
}

// Re-export from content-constants (only used by server components)
export { getGroupsForComponent };
