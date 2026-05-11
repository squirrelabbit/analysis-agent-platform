import type { Prompt } from "@/features/prompt/types/prompt";

// ── 프롬프트 목록 ──────────────────────────────────────────
export const MOCK_PROMPTS: Prompt[] = [
  {
    id: "1",
    version: "0.1",
    contentHash: "",
    createdAt: "",
    operation: "prepare",
    status: "ready",
    summary: "",
    title: "텍스트 전처리",
    updatedAt: "",
    content: `당신은 한국어 텍스트 전처리 전문가입니다.

다음 텍스트를 분석하고 전처리하세요:

1. 불필요한 특수문자, HTML 태그, URL을 제거하세요
2. 이모지와 특수기호를 정리하세요  
3. 반복되는 공백과 줄바꿈을 정규화하세요
4. 원문의 의미를 최대한 보존하면서 정제하세요

출력 형식:
- normalized_text: 정제된 텍스트
- skip: 처리 불가능한 경우 true`,
  },
  {
    id: "2",
    version: "0.3",
    contentHash: "",
    createdAt: "",
    operation: "prepare",
    status: "ready",
    summary: "",
    title: "",
    updatedAt: "",
    content: "",
  },
  {
    id: "3",
    version: "0.4",
    contentHash: "",
    createdAt: "",
    operation: "prepare",
    status: "ready",
    summary: "",
    title: "",
    updatedAt: "",
    content: "",
  },
];
