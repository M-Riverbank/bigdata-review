'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { getComponent } from '@/lib/components';
import { ChevronRight } from 'lucide-react';

export default function Topbar() {
  const pathname = usePathname();
  const segments = pathname.split('/').filter(Boolean);

  // Build breadcrumb items
  const breadcrumbs: { label: string; href: string }[] = [{ label: '学习概览', href: '/' }];

  if (segments.length > 0) {
    let currentPath = '';
    segments.forEach((seg, i) => {
      currentPath += '/' + seg;
      let label = '';

      if (i === 0) {
        if (seg === 'quiz') {
          label = '题库总览';
          breadcrumbs.push({ label, href: currentPath });
        } else if (seg === 'wrong') {
          label = '错题本';
          breadcrumbs.push({ label, href: currentPath });
        } else {
          const comp = getComponent(seg);
          if (comp) {
            label = comp.subLabel;
            breadcrumbs.push({ label, href: currentPath });
          }
        }
      } else if (i === 1) {
        if (segments[0] === 'quiz') {
          const comp = getComponent(seg);
          label = comp ? comp.subLabel + ' · 答题' : seg;
        } else {
          // Article slug under a component
          label = '教程详情';
        }
        breadcrumbs.push({ label, href: currentPath });
      }
    });
  }

  return (
    <header className="sticky top-0 z-30 h-14 bg-gray-950/80 backdrop-blur-sm border-b border-gray-800 flex items-center px-6">
      {/* Breadcrumbs */}
      <nav className="flex items-center gap-1.5 text-sm">
        {breadcrumbs.map((bc, i) => (
          <span key={bc.href} className="flex items-center gap-1.5">
            {i > 0 && <ChevronRight size={14} className="text-gray-600" />}
            {i < breadcrumbs.length - 1 ? (
              <Link
                href={bc.href}
                className="text-gray-500 hover:text-gray-300 transition-colors"
              >
                {bc.label}
              </Link>
            ) : (
              <span className="text-gray-200 font-medium">{bc.label}</span>
            )}
          </span>
        ))}
      </nav>

      {/* Right side: progress (placeholder for now) */}
      <div className="ml-auto" />
    </header>
  );
}
