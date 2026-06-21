import { Article } from '@/types';

// Difficulty mapping by order prefix
export const ARTICLE_DIFFICULTY: Record<string, Article['difficulty']> = {
  '01': '入门', '02': '入门', '03': '入门', '04': '入门',
  '05': '基础', '06': '基础', '07': '基础', '08': '基础',
  '09': '基础', '10': '进阶', '11': '进阶', '12': '进阶',
  '13': '高阶', '14': '综合',
};

export interface GroupDef {
  id: string;
  label: string;
  articles: string[];
}

// Per-component ordered group definitions
export const COMPONENT_GROUPS: Record<string, GroupDef[]> = {
  scala: [
    { id: 'scala-basics', label: 'Scala 基础', articles: ['01', '02', '03', '04'] },
    { id: 'oop', label: '面向对象', articles: ['05', '06', '07'] },
    { id: 'trait-pattern-match', label: 'Trait 与模式匹配', articles: ['08', '09'] },
    { id: 'collections-fp', label: '集合与函数式编程', articles: ['10', '11', '12'] },
    { id: 'implicit-bigdata', label: '隐式转换与大数据实战', articles: ['13', '14'] },
  ],
  'spark-core': [
    { id: 'spark-core-basics', label: 'Spark 入门', articles: ['01', '02'] },
    { id: 'rdd-core', label: 'RDD 核心原理', articles: ['03', '04', '05'] },
    { id: 'shuffle-sched', label: 'Shuffle 与调度', articles: ['06', '07', '08', '09'] },
    { id: 'tuning', label: '调优与部署', articles: ['10', '11', '12'] },
  ],
  'spark-sql': [
    { id: 'sql-basics', label: 'Spark SQL 入门', articles: ['01', '02', '03'] },
    { id: 'sql-advanced', label: 'SQL 高级特性', articles: ['04', '05', '06'] },
    { id: 'sql-tuning', label: '性能优化与实战', articles: ['07', '08', '09', '10'] },
  ],
  'spark-mllib': [
    { id: 'mllib-basics', label: 'MLlib 基础', articles: ['01', '02'] },
    { id: 'feature-model', label: '特征工程与模型', articles: ['03', '04', '05'] },
    { id: 'mllib-advanced', label: '高级算法与部署', articles: ['06', '07', '08'] },
  ],
};

// Helper: get groups for a specific component
export function getGroupsForComponent(component: string): GroupDef[] {
  return COMPONENT_GROUPS[component] || [];
}
