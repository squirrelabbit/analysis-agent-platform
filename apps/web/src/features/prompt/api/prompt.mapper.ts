import type { PromptFormValues } from "../schema/prompt.schema";
import type { Prompt } from "../types/prompt";
import type { PromptPayload, PromptResponse } from "../types/prompt.dto";

export const mapPrompt = (dto: PromptResponse): Prompt => ({
  id: dto.prompt_id,
  version: dto.version,
  operation: dto.operation,
  title: dto.title,
  status: dto.status,
  summary: dto.summary,
  content: dto.content,
  contentHash: dto.content_hash,
  createdAt: dto.created_at,
  updatedAt: dto.updated_at,
});

export const mapPromptFormToRequest = (
  form: PromptFormValues,
): PromptPayload => ({
  version: form.version,
  operation: form.mode === 'single' ? form.type : `${form.type}_${form.mode}`,
  content: form.content
});