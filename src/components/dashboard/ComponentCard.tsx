import { TechComponent } from '@/types';
import Link from 'next/link';
import {
  Code2,
  Flame,
  Table,
  BrainCircuit,
  HardDrive,
  Network,
  Database,
  Rows3,
  Terminal,
  BookOpen,
  HelpCircle,
  ArrowRight,
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

interface ComponentCardProps {
  component: TechComponent;
  articleCount: number;
  questionCount: number;
  progress?: number;
}

export default function ComponentCard({
  component,
  articleCount,
  questionCount,
  progress = 0,
}: ComponentCardProps) {
  const IconComponent = iconMap[component.icon] || BookOpen;

  const colorMap: Record<string, string> = {
    cyan: 'from-cyan-500 to-cyan-400',
    emerald: 'from-emerald-500 to-emerald-400',
    blue: 'from-blue-500 to-blue-400',
    amber: 'from-amber-500 to-amber-400',
    violet: 'from-violet-500 to-violet-400',
    rose: 'from-rose-500 to-rose-400',
  };

  const bgColorMap: Record<string, string> = {
    cyan: 'bg-cyan-500/10 text-cyan-400 border-cyan-500/30',
    emerald: 'bg-emerald-500/10 text-emerald-400 border-emerald-500/30',
    blue: 'bg-blue-500/10 text-blue-400 border-blue-500/30',
    amber: 'bg-amber-500/10 text-amber-400 border-amber-500/30',
    violet: 'bg-violet-500/10 text-violet-400 border-violet-500/30',
    rose: 'bg-rose-500/10 text-rose-400 border-rose-500/30',
  };

  const gradient = colorMap[component.color] || colorMap.emerald;
  const tagStyle = bgColorMap[component.color] || bgColorMap.emerald;

  return (
    <Link
      href={`/${component.id}`}
      className="group block bg-gray-900 border border-gray-800 rounded-xl overflow-hidden transition-all hover:border-gray-700 hover:shadow-lg hover:shadow-black/30 hover:-translate-y-0.5"
    >
      {/* Color top bar */}
      <div className={`h-1 bg-gradient-to-r ${gradient}`} />

      <div className="p-5">
        {/* Header */}
        <div className="flex items-start justify-between mb-3">
          <div className={`p-2.5 rounded-lg border ${tagStyle}`}>
            <IconComponent size={22} />
          </div>
          <ArrowRight
            size={16}
            className="text-gray-600 group-hover:text-gray-400 transition-colors mt-1"
          />
        </div>

        {/* Title */}
        <div className="mb-2">
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-0.5">
            {component.categoryLabel}
          </div>
          <h3 className="text-lg font-semibold text-gray-100">{component.subLabel}</h3>
        </div>

        {/* Description */}
        <p className="text-sm text-gray-400 leading-relaxed mb-4 line-clamp-2">
          {component.description}
        </p>

        {/* Stats */}
        <div className="flex items-center gap-4 mb-4 text-xs text-gray-500">
          <span className="flex items-center gap-1">
            <BookOpen size={13} />
            {articleCount} 篇教程
          </span>
          <span className="flex items-center gap-1">
            <HelpCircle size={13} />
            {questionCount} 道题目
          </span>
        </div>

        {/* Progress bar */}
        {progress > 0 && (
          <div>
            <div className="flex items-center justify-between text-xs mb-1">
              <span className="text-gray-500">学习进度</span>
              <span className="text-gray-400">{Math.round(progress)}%</span>
            </div>
            <div className="h-1.5 bg-gray-800 rounded-full overflow-hidden">
              <div
                className={`h-full bg-gradient-to-r ${gradient} rounded-full transition-all duration-500`}
                style={{ width: `${progress}%` }}
              />
            </div>
          </div>
        )}
      </div>
    </Link>
  );
}
