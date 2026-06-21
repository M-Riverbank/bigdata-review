import OpenAI from 'openai';
import { JudgeRequest, JudgeResponse } from '@/types';

const client = new OpenAI({
  apiKey: process.env.DEEPSEEK_API_KEY,
  baseURL: 'https://api.deepseek.com',
});

export async function judgeAnswer(req: JudgeRequest): Promise<JudgeResponse> {
  const isWriting = req.questionType === 'writing';

  const systemPrompt = isWriting
    ? `你是一位严格但公正的大数据面试官，负责评判 Spark SQL 笔试题。请根据以下标准评判学生的 SQL 回答：

1. 对照参考要点，判断学生的 SQL 是否满足了题目要求
2. 不完全要求逐字匹配，语义等价（能得出相同结果）即可给满分
3. 如果 SQL 有语法错误要明确指出
4. 如果遗漏了关键条件或逻辑有误，要提示补充
5. 除非题目明确要求"优化性能"或"考虑效率"，否则不要因为性能问题扣分

返回 JSON 格式：
{
  "score": 0-100 的整数分数,
  "feedback": "详细点评，包含：哪些写对了、哪些遗漏了、哪些有误、改进建议",
  "isPass": score >= 60 ? true : false
}

只返回 JSON，不要包含其他文字。`
    : `你是一位严格但公正的大数据面试官。请根据以下标准评判学生的简答题回答：

1. 对照参考要点，判断学生是否覆盖了核心概念
2. 不完全要求逐字匹配，意思正确即可给分
3. 如果回答有错误理解，要明确指出
4. 如果遗漏了重要要点，要提示补充

返回 JSON 格式：
{
  "score": 0-100 的整数分数,
  "feedback": "详细点评，包含：哪些说对了、哪些遗漏了、哪些理解有偏差、改进建议",
  "isPass": score >= 60 ? true : false
}

只返回 JSON，不要包含其他文字。`;

  const userPrompt = isWriting && req.tables
    ? `题目：${req.stem}

涉及的表结构：
${req.tables.map(t => `表名：${t.name}
${t.schema}${t.data ? '\n样例数据：\n' + t.data : ''}`).join('\n\n')}

参考要点（判分依据）：
${req.referencePoints.map((p, i) => `${i + 1}. ${p}`).join('\n')}

学生回答：
${req.userAnswer}

请评判。`
    : `题目：${req.stem}

参考要点（判分依据）：
${req.referencePoints.map((p, i) => `${i + 1}. ${p}`).join('\n')}

学生回答：
${req.userAnswer}

请评判。`;

  const completion = await client.chat.completions.create({
    model: 'deepseek-chat',
    temperature: 0,
    messages: [
      { role: 'system', content: systemPrompt },
      { role: 'user', content: userPrompt },
    ],
  });

  const content = completion.choices[0]?.message?.content || '';
  // Try to parse JSON from response (handle potential markdown code block wrapping)
  const jsonMatch = content.match(/\{[\s\S]*\}/);
  if (!jsonMatch) {
    throw new Error('AI 返回格式异常，请重试');
  }

  const result = JSON.parse(jsonMatch[0]) as JudgeResponse;
  return result;
}
