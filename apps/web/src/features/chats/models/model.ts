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

export interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  createdAt: string;
  display?: ChatDisplay;
  plan?: ChatPlan;
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
