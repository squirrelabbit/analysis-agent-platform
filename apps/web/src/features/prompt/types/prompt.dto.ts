import type { Operation } from "./prompt";

export interface PromptResponse {
  prompt_id: string;
  version: string;
  operation: string;
  title: string;
  status: string;
  summary: string;
  content: string;
  content_hash: string;
  created_at: string;
  updated_at: string;
}

export interface PromptListResponse {
  items: PromptResponse[]
}

export interface PromptCatalogResponse {
  source_path: string,
  items: {
    version: string,
    title: string,
    operation: string,
    status: string,
    summary: string,
    default_gourps: string[]
  }[];
}

export interface PromptPayload {
  version: string;
  operation: Operation;
  content: string;
}
