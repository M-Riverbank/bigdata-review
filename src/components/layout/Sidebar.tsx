'use client';

import { useState } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { techComponents, getAllCategoryLabels } from '@/lib/components';
import {
  LayoutDashboard,
  BookOpen,
  HelpCircle,
  XCircle,
  ChevronDown,
  ChevronRight,
  Flame,
  HardDrive,
  Database,
  Rows3,
  Terminal,
  Code2,
  Network,
  Table,
  BrainCircuit,
  GraduationCap,
} from 'lucide-react';

const iconMap: Record<string, React.ComponentType<{ size?: number; className?: string }>> = {
  Code2,
  Flame,
  Table,
  BrainCircuit,
  HardDrive,
  Network,
  Database,
  Rows3,
  Terminal,
};

const categoryIcons: Record<string, React.ComponentType<{ size?: number; className?: string }>> = {
  scala: Code2,
  spark: Flame,
  hadoop: HardDrive,
  hive: Database,
  hbase: Rows3,
  mysql: Terminal,
};

export default function Sidebar() {
  const pathname = usePathname();
  const [expandedCategories, setExpandedCategories] = useState<Set<string>>(new Set());

  const toggleCategory = (cat: string) => {
    setExpandedCategories(prev => {
      const next = new Set(prev);
      if (next.has(cat)) next.delete(cat);
      else next.add(cat);
      return next;
    });
  };

  const isActive = (href: string) => pathname === href || pathname.startsWith(href + '/');

  const categories = getAllCategoryLabels();

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
          <span>导航大盘</span>
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
          const isExpanded = expandedCategories.has(category);
          const CatIcon = categoryIcons[category] || BookOpen;
          const subComponents = techComponents.filter(c => c.category === category);
          const hasActiveChild = subComponents.some(c => isActive(`/${c.id}`));

          return (
            <div key={category}>
              {/* Category header */}
              <button
                onClick={() => toggleCategory(category)}
                className={`w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm transition-colors ${
                  hasActiveChild
                    ? 'text-gray-200'
                    : 'text-gray-400 hover:text-gray-200 hover:bg-gray-800/50'
                }`}
              >
                <CatIcon size={18} />
                <span className="flex-1 text-left font-medium">{label}</span>
                {isExpanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
              </button>

              {/* Sub items */}
              {isExpanded && (
                <div className="ml-7 mt-0.5 space-y-0.5 border-l border-gray-800 pl-3">
                  {subComponents.map(sub => (
                    <Link
                      key={sub.id}
                      href={`/${sub.id}`}
                      className={`block px-3 py-2 rounded-lg text-sm transition-colors ${
                        isActive(`/${sub.id}`)
                          ? 'text-emerald-400 bg-emerald-500/10 font-medium'
                          : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800/50'
                      }`}
                    >
                      {sub.subLabel === label ? '概述' : sub.subLabel.replace(label + ' ', '')}
                    </Link>
                  ))}
                  {/* Quiz link for this category */}
                  {subComponents.map(sub => (
                    <Link
                      key={`quiz-${sub.id}`}
                      href={`/quiz/${sub.id}`}
                      className={`block px-3 py-2 rounded-lg text-sm transition-colors ${
                        isActive(`/quiz/${sub.id}`)
                          ? 'text-amber-400 bg-amber-500/10 font-medium'
                          : 'text-gray-500 hover:text-gray-300 hover:bg-gray-800/50'
                      }`}
                    >
                      题库 · {sub.subLabel === label ? sub.subLabel : sub.subLabel.replace(label + ' ', '')}
                    </Link>
                  ))}
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
