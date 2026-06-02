// GET /prompt_options?task={task} 응답 — 백엔드가 사용 가능한 prompt
// version 목록과 default를 내려준다 (silverone 백엔드 핸드오프 2026-06-02).
export type PromptOptionsTask = "doc_genuineness" | "clause_label";

export interface PromptVersionDto {
  version: string;
  label: string;
}

export interface PromptOptionsResponseDto {
  task: PromptOptionsTask;
  default: string;
  versions: PromptVersionDto[];
}
