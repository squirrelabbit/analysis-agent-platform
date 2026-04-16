import type { Operation } from ".."

// GET /projects/:project_id/prompts — response data
export interface PromptResponse {
  project_id: string
  version: string
  operation: Operation
  title: string
  status: string
  summary: string
  content: string
  content_hash: string
  created_at: string
  updated_at: string
}

// GET /projects/:project_id/prompts — response data
export interface PromptListResponse {
  items: PromptResponse[]
}

// POST /projects/:project_id/prompts — body
export interface CreatePromptPayload {
  version: string
  operation: Operation
  content: string
}