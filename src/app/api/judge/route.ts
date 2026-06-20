import { NextRequest, NextResponse } from 'next/server';
import { judgeAnswer } from '@/lib/deepseek';

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { stem, userAnswer, referencePoints } = body;

    if (!stem || !userAnswer || !referencePoints) {
      return NextResponse.json(
        { error: '缺少必填字段：stem, userAnswer, referencePoints' },
        { status: 400 }
      );
    }

    const result = await judgeAnswer({ stem, userAnswer, referencePoints });
    return NextResponse.json(result);
  } catch (error: any) {
    console.error('AI judge error:', error);
    return NextResponse.json(
      { error: error.message || 'AI 判题服务异常，请稍后重试' },
      { status: 500 }
    );
  }
}
