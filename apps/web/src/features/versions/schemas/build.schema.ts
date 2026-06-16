import { z } from "zod";

const textColumnSchema = z.object({
  value: z.string().min(1, "컬럼명을 입력하세요"),
});

export const CleanSchema = z.object({
  textColumns: z.array(textColumnSchema).optional(),
  outputPath: z.string().optional(),
  cleanOptions: z.object({
    removeEnglish: z.boolean(),
    removeNumbers: z.boolean(),
    removeSpecial: z.boolean(),
    removeMonosyllables: z.boolean(),
  }).optional(),
  force: z.boolean().optional(),
});

export const GenuinenessSchema = z.object({
  promptVersion: z.string().optional(),
});

export const ClauseSchema = z.object({
  promptVersion: z.string().optional(),
  includeGenuineness: z.array(z.string()).optional(), // doc_genuineness ready 필요
});

export type CleanFormValues = z.infer<typeof CleanSchema>;
export type GenuinenessFormValues = z.infer<typeof GenuinenessSchema>;
export type ClauseFormValues = z.infer<typeof ClauseSchema>;

export const BuildCleanSchema = z.object({
  textColumns: z.array(textColumnSchema).optional(),
  // outputPath: z.string().optional(),
  // cleanOptions: z.object({
  //   removeEnglish: z.boolean(),
  //   removeNumbers: z.boolean(),
  //   removeSpecial: z.boolean(),
  //   removeMonosyllables: z.boolean(),
  // }),
});

export const BuildGenuinenessSchema = z.object({
  promptVersion: z.string().min(1, "프롬프트 버전을 선택하세요"),
  modelId: z.string().optional(), // 빈 값 = env default 모델 (LLOA_MODEL)
  verify: z.boolean().optional(), // 교차검증 모드 (ADR-026)
});

export const BuildClauseSchema = z.object({
  promptVersion: z.string().min(1, "프롬프트 버전을 선택하세요"),
  includeGenuineness: z.array(z.string()).optional(), // doc_genuineness ready 필요
  modelId: z.string().optional(), // 빈 값 = env default 모델 (LLOA_MODEL)
});

export const BuildKeywordSchema = z.object({
  keywordMinLen: z.number().optional(),
});

export type BuildCleanFormValues = z.infer<typeof BuildCleanSchema>;
export type BuildGenuinenessFormValues = z.infer<typeof BuildGenuinenessSchema>;
export type BuildClauseFormValues = z.infer<typeof BuildClauseSchema>;
export type BuildKeywordFormValues = z.infer<typeof BuildKeywordSchema>;
