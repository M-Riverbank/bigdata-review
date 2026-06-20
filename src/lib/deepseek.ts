import OpenAI from 'openai';
import { JudgeRequest, JudgeResponse } from '@/types';

const client = new OpenAI({
  apiKey: process.env.DEEPSEEK_API_KEY,
  baseURL: 'https://api.deepseek.com',
});

export async function judgeAnswer(req: JudgeRequest): Promise<JudgeResponse> {
  const systemPrompt = `你是一位严格但公正的大数据面试官。请根据以下标准评判学生的简答题回答：

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

  const userPrompt = `题目：${req.stem}

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
