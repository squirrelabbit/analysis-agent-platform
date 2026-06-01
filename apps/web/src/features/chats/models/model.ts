import type { ChatRole } from "./dto";

export interface ChatTableDisplay {
  type: "table";
  title?: string;
  columns: string[];
  rows: Record<string, unknown>[];
}

export type ChatDisplay = ChatTableDisplay;

export interface ChatPlanStep {
  id: string;
  skill: string;
  params: Record<string, unknown>;
  label?: string;
  expression?: string;
}

export interface ChatPlan {
  version: string;
  steps: ChatPlanStep[];
}

export interface ChatChart {
  kind: "bar" | "line";
  x: string;
  // 1차 구현: 단일 numeric series 우선. 응답이 array면 매퍼에서 첫 값으로 좁힌다.
  y: string;
  title?: string;
  rows: Record<string, unknown>[];
}

export type TaxonomyStatus =
  | "ok"
  | "legacy_missing"
  | "hash_mismatch"
  | "id_mismatch";

// 메인 렌더 view. 백엔드가 새 타입(metric, stacked_bar 등)을 추가할 수 있어
// 알려진 값만 좁히고 그 외는 "unknown"으로 떨어뜨려 table fallback한다.
export type RecommendedView = "table" | "bar" | "line" | "unknown";

export interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  createdAt: string;
  display?: ChatDisplay;
  plan?: ChatPlan;
  chart?: ChatChart;
  warnings?: string[];
  taxonomyStatus?: TaxonomyStatus;
  // 백엔드가 chart를 추천했지만 유효 데이터 부족 등으로 매퍼가 chart를
  // 생성하지 못한 경우에 fallback 안내를 띄우기 위한 플래그.
  chartFallbackReason?: "insufficient_data";
  recommendedView?: RecommendedView;
}

export interface ChatThread {
  id: string;
  title: string;
  lastMessage: string;
  messageCount: number;
  updatedAt: string;
}

export interface ChatThreadDetail {
  id: string;
  messages: ChatMessage[];
}

export interface AnalyzeResult {
  threadId: string;
  userMessage?: ChatMessage;
  assistantMessage?: ChatMessage;
  errorMessage?: string | null;
}
