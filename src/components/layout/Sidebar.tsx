'use client';

import { useState, useEffect, useMemo, useCallback } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { techComponents, getAllCategoryLabels } from '@/lib/components';
import { getGroupsForComponent } from '@/lib/content-constants';
import { Article } from '@/types';
import {
  LayoutDashboard,
  BookOpen,
  HelpCircle,
  XCircle,
  ChevronDown,
  ChevronRight,
  GraduationCap,
} from 'lucide-react';
import {
  SiScala,
  SiApachespark,
  SiApachehadoop,
  SiApachehive,
  SiApachehbase,
  SiMysql,
} from 'react-icons/si';

// Brand icons per category
const categoryIcons: Record<string, React.ComponentType<{ size?: number; className?: string }>> = {
  scala: SiScala,
  spark: SiApachespark,
  hadoop: SiApachehadoop,
  hive: SiApachehive,
  hbase: SiApachehbase,
  mysql: SiMysql,
};

// Difficulty badge styles
const difficultyStyles: Record<string, { bg: string; text: string; label: string }> = {
  '入门': { bg: 'bg-emerald-500/10', text: 'text-emerald-400', label: '入门' },
  '基础': { bg: 'bg-yellow-500/10', text: 'text-yellow-400', label: '基础' },
  '进阶': { bg: 'bg-orange-500/10', text: 'text-orange-400', label: '进阶' },
  '高阶': { bg: 'bg-red-500/10', text: 'text-red-400', label: '高阶' },
  '综合': { bg: 'bg-purple-500/10', text: 'text-purple-400', label: '综合' },
};

interface SidebarProps {
  articles: Article[];
}

// Compute which categories and groups should be auto-expanded based on pathname
function computeAutoExpand(pathname: string, articles: Article[]): Set<string> {
  const auto = new Set<string>();

  // Check if on a component page
  for (const c of techComponents) {
    if (pathname.startsWith(`/${c.id}`)) {
      auto.add(c.category);
      // If on an article page, auto-expand the relevant group
      if (pathname.startsWith(`/${c.id}/`)) {
        const slug = pathname.split('/').pop() || '';
        const article = articles.find(a => a.component === c.id && a.slug === slug);
        if (article) {
          auto.add(`${c.category}-${c.id}-${article.group}`);
        }
      }
      break;
    }
  }

  // Auto-expand category for quiz pages
  if (pathname.startsWith('/quiz/')) {
    const compId = pathname.split('/')[2];
    const comp = techComponents.find(c => c.id === compId);
    if (comp) auto.add(comp.category);
  }

  return auto;
}

export default function Sidebar({ articles }: SidebarProps) {
  const pathname = usePathname();
  const [userToggled, setUserToggled] = useState<Set<string>>(new Set());

  // Load persisted user-toggled state on mount
  useEffect(() => {
    try {
      const raw = localStorage.getItem('sidebar-expanded');
      if (raw) setUserToggled(new Set(JSON.parse(raw)));
    } catch {}
  }, []);

  // Auto-expanded from pathname + user toggles → effective expanded state
  const effectiveExpanded = useMemo(() => {
    const auto = computeAutoExpand(pathname, articles);
    const merged = new Set([...auto, ...userToggled]);
    return merged;
  }, [pathname, articles, userToggled]);

  // Persist user toggles
  const persistUserToggled = useCallback((next: Set<string>) => {
    setUserToggled(next);
    try {
      localStorage.setItem('sidebar-expanded', JSON.stringify(Array.from(next)));
    } catch {}
  }, []);

  const toggleCategory = (cat: string) => {
    setUserToggled(prev => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      persistUserToggled(next);
      return next;
    });
  };

  const toggleGroup = (groupId: string) => {
    setUserToggled(prev => {
      const next = new Set(prev);
      if (next.has(groupId)) next.delete(groupId);
      else next.add(groupId);
      persistUserToggled(next);
      return next;
    });
  };

  const isActive = (href: string) => pathname === href || pathname.startsWith(href + '/');

  const categories = getAllCategoryLabels();

  // Group articles by component
  const articlesByComponent = useMemo(() => {
    const map: Record<string, Article[]> = {};
    articles.forEach(a => {
      if (!map[a.component]) map[a.component] = [];
      map[a.component].push(a);
    });
    return map;
  }, [articles]);

  return (
    <aside className="fixed left-0 top-0 z-40 h-screen w-60 bg-gray-950 border-r border-gray-800 flex flex-col">
      {/* Logo */}
      <Link href="/" className="flex items-center gap-3 px-5 py-5 border-b border-gray-800">
        <GraduationCap size={28} className="text-emerald-400" />
        <div>
          <div className="text-sm font-bold text-gray-100 leading-tight">大数据面试</div>
          <div className="text-sm font-bold text-emerald-400 leading-tight">备战冲刺网</div>
        </div>
      </Link>

      {/* Nav items */}
      <nav className="flex-1 overflow-y-auto py-3 px-3 space-y-1">
        {/* Dashboard */}
        <Link
          href="/"
          className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors ${
            pathname === '/'
              ? 'bg-emerald-500/10 text-emerald-400 font-medium'
              : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/50'
          }`}
        >
          <LayoutDashboard size={18} />
          <span>学习概览</span>
        </Link>

        {/* Quiz overview */}
        <Link
          href="/quiz"
          className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors ${
            isActive('/quiz') && pathname.split('/').length === 2
              ? 'bg-emerald-500/10 text-emerald-400 font-medium'
              : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/50'
          }`}
        >
          <HelpCircle size={18} />
          <span>题库总览</span>
        </Link>

        {/* Wrong questions */}
        <Link
          href="/wrong"
          className={`flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors ${
            pathname === '/wrong'
              ? 'bg-emerald-500/10 text-emerald-400 font-medium'
              : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/50'
          }`}
        >
          <XCircle size={18} />
          <span>错题本</span>
        </Link>

        {/* Divider */}
        <div className="pt-3 pb-1">
          <div className="px-3 text-xs font-semibold text-gray-500 uppercase tracking-wider">
            技术组件
          </div>
        </div>

        {/* Category groups */}
        {categories.map(({ category, label }) => {
          const isExpanded = effectiveExpanded.has(category);
          const CatIcon = categoryIcons[category] || BookOpen;
          const subComponents = techComponents.filter(c => c.category === category);

          // Check if any sub-component has defined groups
          const hasGroups = subComponents.some(c => getGroupsForComponent(c.id).length > 0);

          return (
            <div key={category}>
              {/* Category header — text-sm (14px), font-semibold */}
              <button
                onClick={() => toggleCategory(category)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors hover:bg-gray-800/50 ${
                  isExpanded || subComponents.some(c => isActive(`/${c.id}`))
                    ? 'text-gray-200'
                    : 'text-gray-400 hover:text-gray-200'
                }`}
              >
                <CatIcon size={18} className="flex-shrink-0" />
                <span className="flex-1 text-left font-semibold">{label}</span>
                {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              </button>

              {/* Sub items */}
              {isExpanded && (
                <div className="ml-2 mt-0.5 space-y-0.5 border-l border-gray-800 pl-2">
                  {subComponents.map(sub => {
                    const groups = getGroupsForComponent(sub.id);

                    if (groups.length > 0) {
                      // === Component with article groups (nested) ===
                      const compArticles = articlesByComponent[sub.id] || [];
                      const isCompActive = isActive(`/${sub.id}`);

                      return (
                        <div key={sub.id} className="mb-1">
                          {/* Sub-component label — text-[13px], font-medium */}
                          <div className={`px-2 py-1.5 text-[13px] font-medium transition-colors rounded ${
                            isCompActive ? 'text-emerald-400' : 'text-gray-400'
                          }`}>
                            {sub.subLabel}
                          </div>

                          {/* Groups */}
                          <div className="ml-2 space-y-0.5">
                            {groups.map(group => {
                              const groupArticles = compArticles
                                .filter(a => group.articles.includes(String(a.order).padStart(2, '0')))
                                .sort((a, b) => a.order - b.order);
                              if (groupArticles.length === 0) return null;

                              const groupKey = `${category}-${sub.id}-${group.id}`;
                              const isGroupExpanded = effectiveExpanded.has(groupKey);

                              return (
                                <div key={group.id}>
                                  {/* Group header — text-xs (12px), text-gray-500 */}
                                  <button
                                    onClick={() => toggleGroup(groupKey)}
                                    className="w-full flex items-center gap-1.5 px-2 py-1 text-xs transition-colors hover:bg-gray-800/30 rounded text-gray-500 hover:text-gray-300"
                                  >
                                    {isGroupExpanded ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                                    <span>{group.label}</span>
                                    <span className="text-gray-600">({groupArticles.length})</span>
                                  </button>

                                  {/* Articles — text-[11px], text-gray-500 */}
                                  {isGroupExpanded && groupArticles.map(article => {
                                    const diffStyle = difficultyStyles[article.difficulty] || difficultyStyles['基础'];
                                    const globalIdx = String(article.order).padStart(2, '0');
                                    const isArticleActive = isActive(`/${sub.id}/${article.slug}`);

                                    return (
                                      <Link
                                        key={article.slug}
                                        href={`/${sub.id}/${article.slug}`}
                                        className={`flex items-center gap-2 ml-4 py-1 rounded-lg text-[11px] transition-colors ${
                                          isArticleActive
                                            ? 'text-emerald-400 bg-emerald-500/10 font-medium'
                                            : 'text-gray-400 hover:text-gray-300 hover:bg-gray-800/30'
                                        }`}
                                      >
                                        <span className="text-gray-600 font-mono w-4">{globalIdx}</span>
                                        <span className="flex-1 truncate">{article.title.split('—').pop()?.trim() || article.title}</span>
                                        <span className={`px-1 py-0.5 rounded text-[9px] ${diffStyle.bg} ${diffStyle.text} flex-shrink-0`}>
                                          {diffStyle.label}
                                        </span>
                                      </Link>
                                    );
                                  })}
                                </div>
                              );
                            })}
                          </div>

                          {/* Quiz link for this component — text-xs */}
                          <Link
                            href={`/quiz/${sub.id}`}
                            className={`block px-2 py-1.5 mt-0.5 rounded text-xs transition-colors ${
                              isActive(`/quiz/${sub.id}`)
                                ? 'text-amber-400 bg-amber-500/10 font-medium'
                                : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800/30'
                            }`}
                          >
                            🎯 题库
                          </Link>
                        </div>
                      );
                    } else {
                      // === Component without groups (flat rendering) ===
                      const isCompActive = isActive(`/${sub.id}`);

                      return (
                        <div key={sub.id} className="mb-1">
                          {/* Sub-component name — text-[13px], font-medium */}
                          <Link
                            href={`/${sub.id}`}
                            className={`block px-2 py-1.5 text-[13px] font-medium rounded transition-colors ${
                              isCompActive && pathname.split('/').length === 2
                                ? 'text-emerald-400 bg-emerald-500/10'
                                : 'text-gray-400 hover:text-gray-300 hover:bg-gray-800/30'
                            }`}
                          >
                            {sub.subLabel}
                          </Link>

                          {/* Quiz link for this component — text-xs */}
                          <Link
                            href={`/quiz/${sub.id}`}
                            className={`block px-2 py-1.5 rounded text-xs transition-colors ${
                              isActive(`/quiz/${sub.id}`)
                                ? 'text-amber-400 bg-amber-500/10 font-medium'
                                : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800/30'
                            }`}
                          >
                            🎯 题库
                          </Link>
                        </div>
                      );
                    }
                  })}
                </div>
              )}
            </div>
          );
        })}
      </nav>

      {/* Footer */}
      <div className="px-5 py-4 border-t border-gray-800 text-xs text-gray-600">
        大数据面试备战冲刺网 v1.0
      </div>
    </aside>
  );
}
