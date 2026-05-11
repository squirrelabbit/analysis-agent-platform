import { z } from "zod"

export const promptSchema = z.object({
  version: z.string().min(1, "버전 입력은 필수입니다"),
  type: z.enum(['prepare','sentiment',]),
  mode: z.enum(['single','batch',]),
  // operation: z.enum(['prepare', 'prepare_batch', 'sentiment', 'sentiment_batch']),
  content: z.string().min(1, "프롬프트 내용 입력은 필수입니다"),
})

export type PromptFormValues = z.infer<typeof promptSchema>