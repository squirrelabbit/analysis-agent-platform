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
