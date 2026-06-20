import { TechComponent } from '@/types';

export const techComponents: TechComponent[] = [
  {
    id: 'scala',
    category: 'scala',
    categoryLabel: 'Scala',
    subLabel: 'Scala',
    description: '掌握函数式编程、隐式转换、模式匹配等大数据基础语言核心特性',
    icon: 'Code2',
    color: 'cyan',
    topics: ['基础语法', '函数式编程', '隐式转换', '模式匹配', '集合操作', '隐式参数'],
  },
  {
    id: 'spark-core',
    category: 'spark',
    categoryLabel: 'Spark',
    subLabel: 'Spark Core',
    description: '深入理解 RDD 弹性分布式数据集、共享变量、任务调度与内存管理',
    icon: 'Flame',
    color: 'emerald',
    topics: ['RDD 算子', '共享变量', '任务调度', 'Shuffle 原理', '内存管理'],
  },
  {
    id: 'spark-sql',
    category: 'spark',
    categoryLabel: 'Spark',
    subLabel: 'Spark SQL',
    description: '精通 DataFrame/DataSet API、窗口函数、多表 Join 策略与 Catalyst 优化器',
    icon: 'Table',
    color: 'emerald',
    topics: ['DataFrame API', '窗口函数', 'Join 策略', 'Catalyst 优化器', 'UDF/UDAF'],
  },
  {
    id: 'spark-mllib',
    category: 'spark',
    categoryLabel: 'Spark',
    subLabel: 'Spark MLlib',
    description: '掌握常用机器学习算法在 Spark 上的分布式实现与特征工程',
    icon: 'BrainCircuit',
    color: 'emerald',
    topics: ['特征工程', '分类回归', '聚类', '推荐算法', 'Pipeline'],
  },
  {
    id: 'hdfs',
    category: 'hadoop',
    categoryLabel: 'Hadoop',
    subLabel: 'HDFS',
    description: '深入 HDFS 架构原理、读写流程、NameNode 高可用与联邦机制',
    icon: 'HardDrive',
    color: 'blue',
    topics: ['架构原理', '读写流程', 'NameNode/HA', '联邦机制', '纠删码'],
  },
  {
    id: 'yarn',
    category: 'hadoop',
    categoryLabel: 'Hadoop',
    subLabel: 'YARN',
    description: '理解资源调度框架、Capacity/Fair Scheduler、队列管理与调优',
    icon: 'Network',
    color: 'blue',
    topics: ['调度器原理', 'Capacity/Fair', '队列管理', '资源调优', '标签调度'],
  },
  {
    id: 'hive',
    category: 'hive',
    categoryLabel: 'Hive',
    subLabel: 'Hive',
    description: '深入 Hive 架构与执行流程、分区分桶、数仓分层建模方法论',
    icon: 'Database',
    color: 'amber',
    topics: ['架构原理', '分区/分桶', '数仓分层', 'UDF 开发', '调优'],
  },
  {
    id: 'hbase',
    category: 'hbase',
    categoryLabel: 'HBase',
    subLabel: 'HBase',
    description: '掌握 RowKey 设计原则、Region 分裂、LSM Tree 与读写路径',
    icon: 'Rows3',
    color: 'violet',
    topics: ['RowKey 设计', 'Region 管理', 'LSM Tree', '读写流程', 'Phoenix'],
  },
  {
    id: 'mysql',
    category: 'mysql',
    categoryLabel: 'MySQL',
    subLabel: 'MySQL',
    description: '巩固 SQL 基础、索引优化、Explain 执行计划、事务隔离级别与锁机制',
    icon: 'Terminal',
    color: 'rose',
    topics: ['索引优化', 'Explain', '事务隔离', '锁机制', 'SQL 优化'],
  },
];

export function getComponent(id: string): TechComponent | undefined {
  return techComponents.find(c => c.id === id);
}

export function getComponentsByCategory(category: string): TechComponent[] {
  return techComponents.filter(c => c.category === category);
}

export function getAllCategoryLabels(): { category: string; label: string }[] {
  const seen = new Map<string, string>();
  techComponents.forEach(c => {
    if (!seen.has(c.category)) {
      seen.set(c.category, c.categoryLabel);
    }
  });
  return Array.from(seen.entries()).map(([category, label]) => ({ category, label }));
}
