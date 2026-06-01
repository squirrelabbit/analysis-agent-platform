import type { ChatRole } from "./dto";

export interface ChatTableDisplay {
  type: "table";
  title?: string;
  columns: string[];
  rows: Record<string, unknown>[];
}

export type ChatDisplay = ChatTableDisplay;

export interface ChatMessage {
  id: string;
  role: ChatRole;
  content: string;
  createdAt: string;
  display?: ChatDisplay;
}

export interface AnalyzeResult {
  threadId: string;
  assistantMessage?: ChatMessage;
  errorMessage?: string | null;
}
