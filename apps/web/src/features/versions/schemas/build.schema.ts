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
});

export const BuildClauseSchema = z.object({
  promptVersion: z.string().min(1, "프롬프트 버전을 선택하세요"),
  includeGenuineness: z.array(z.string()).optional(), // doc_genuineness ready 필요
});

export type BuildCleanFormValues = z.infer<typeof BuildCleanSchema>;
export type BuildGenuinenessFormValues = z.infer<typeof BuildGenuinenessSchema>;
export type BuildClauseFormValues = z.infer<typeof BuildClauseSchema>;
