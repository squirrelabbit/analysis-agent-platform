import { z } from "zod";

const textColumnSchema = z.object({
  value: z.string().min(1, "컬럼명을 입력하세요"),
});

export const CleanJobSchema = z.object({
  textColumns: z.array(textColumnSchema).optional(),
  outputPath: z.string().optional(),
  cleanOptions: z.object({
    removeEnglish: z.boolean(),
    removeNumbers: z.boolean(),
    removeSpecial: z.boolean(),
    removeMonosyllables: z.boolean(),
  }),
  force: z.boolean().optional(),
});

export const DocGenuinenessJobSchema = z.object({
  promptVersion: z.string().optional(),
});

export const ClauseLabelJobSchema = z.object({
  promptVersion: z.string().optional(),
  includeGenuineness: z.array(z.string()).optional(), // doc_genuineness ready 필요
});

export type CleanJobFormValues = z.infer<typeof CleanJobSchema>;
export type DocGenuinenessJobFormValues = z.infer<typeof DocGenuinenessJobSchema>;
export type ClauseLabelJobFormValues = z.infer<typeof ClauseLabelJobSchema>;
