export type PromptOperation =
  | "prepare"
  | "prepare_batch"
  | "sentiment"
  | "sentiment_batch";

export type PromptStatus = "active" | "ready" | "deprecated";

export interface Prompt {
  id: string;
  version: string;
  operation: PromptOperation;
  title: string;
  status: string;
  summary: string;
  content: string;
  contentHash: string;
  createdAt: string;
  updatedAt: string;
}

// 같은 title 그룹 = 하나의 프롬프트, versions = 버전 목록
export interface PromptGroup {
  groupKey: string;        // title 기준
  title: string;
  operation: PromptOperation;
  latestVersion: string;
  summary: string;
  updatedAt: string;
  versions: Prompt[];      // 최신순 정렬
}