import { ChoiceQuestion, EssayQuestion, WritingQuestion, Question, QuizSet } from '@/types';
import fs from 'fs';
import path from 'path';

const DATA_ROOT = path.join(process.cwd(), 'data');

// Load quiz set for a component
export function getQuizSet(component: string): QuizSet | null {
  const filePath = path.join(DATA_ROOT, `${component}.json`);
  if (!fs.existsSync(filePath)) return null;

  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    const set = JSON.parse(raw) as QuizSet;
    return set;
  } catch {
    return null;
  }
}

// Count questions in a component's quiz
export function countQuestions(component: string): number {
  const set = getQuizSet(component);
  return set ? set.questions.length : 0;
}

// Count questions by type
export function countChoiceQuestions(questions: Question[]): number {
  return questions.filter(q => q.type === 'choice').length;
}

export function countEssayQuestions(questions: Question[]): number {
  return questions.filter(q => q.type === 'essay').length;
}

export function countWritingQuestions(questions: Question[]): number {
  return questions.filter(q => q.type === 'writing').length;
}

// Count by difficulty
export function countByDifficulty(
  questions: Question[],
  difficulty: 'easy' | 'medium' | 'hard'
): number {
  return questions.filter(q => q.difficulty === difficulty).length;
}

// Get total question count across all components
export function getTotalQuestionCount(): Record<string, number> {
  if (!fs.existsSync(DATA_ROOT)) return {};

  const counts: Record<string, number> = {};
  fs.readdirSync(DATA_ROOT)
    .filter(f => f.endsWith('.json'))
    .forEach(f => {
      const componentId = f.replace('.json', '');
      counts[componentId] = countQuestions(componentId);
    });
  return counts;
}
