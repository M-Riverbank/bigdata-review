import { Article } from '@/types';
import fs from 'fs';
import path from 'path';

const CONTENT_ROOT = path.join(process.cwd(), 'content');

// Map component id to content directory path
// Sub-components like 'spark-sql' live under 'spark/sql/'
function getContentDir(componentId: string): string {
  // e.g. 'spark-sql' → 'spark/sql', 'spark-core' → 'spark/core'
  // 'hive' → 'hive', 'scala' → 'scala'
  const parts = componentId.split('-');
  if (parts.length >= 2 && !['hdfs', 'yarn'].includes(componentId)) {
    return path.join(CONTENT_ROOT, parts[0], parts[1]);
  }
  // hadoop/hdfs, hadoop/yarn
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
    // Check if directory itself has .md files (leaf content dir)
    const hasMd = entries.some(e => e.isFile() && e.name.endsWith('.md'));
    if (hasMd) {
      results.push({ relPath: rel, absPath: dir });
    }
    // Also scan subdirectories
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
  // 'spark/sql' → 'spark-sql', 'hive' → 'hive'
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
      const title = match
        ? match[2].replace(/-/g, ' ').replace(/^\S/, s => s.toUpperCase())
        : f.replace(/\.md$/, '');
      return {
        slug: f.replace(/\.md$/, ''),
        title,
        component,
        order,
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
