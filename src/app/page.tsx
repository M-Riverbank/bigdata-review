import { techComponents } from '@/lib/components';
import { countArticles } from '@/lib/content';
import { countQuestions } from '@/lib/quiz';
import { UserProgress } from '@/types';
import DashboardClient from './DashboardClient';

// Server component: reads file system data
export default function DashboardPage() {
  // Pre-compute article and question counts on server
  const componentData = techComponents.map(comp => ({
    component: comp,
    articleCount: countArticles(comp.id),
    questionCount: countQuestions(comp.id),
  }));

  const totalArticles = componentData.reduce((sum, d) => sum + d.articleCount, 0);
  const totalQuestions = componentData.reduce((sum, d) => sum + d.questionCount, 0);

  return (
    <DashboardClient
      componentData={componentData}
      totalArticles={totalArticles}
      totalQuestions={totalQuestions}
    />
  );
}
