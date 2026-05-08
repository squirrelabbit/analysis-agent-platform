import { z } from "zod"

export const datasetSchema = z.object({
  name: z.string().min(1, "이름은 필수입니다"),
  description: z.string().min(1, "설명은 필수입니다"),
  dataType: z.enum(["structured", "unstructured"]),
})

export type DatasetFormValues = z.infer<typeof datasetSchema>

const textColumnSchema = z.object({
  value: z.string().min(1, "컬럼명을 입력하세요"),
})

export const versionSchema = z.object({
  file: z
    .instanceof(File, { message: "파일을 선택하세요" }),
    // .refine((f) => f.size <= 100 * 1024 * 1024, "파일 크기는 100MB 이하여야 합니다"),
  dataType: z.enum(["structured", "unstructured"], {
    error: "데이터 타입을 선택하세요",
  }),
  analysisType: z.enum(["sentiment", "prepare", "embedding"], {
    error: "분석 유형을 선택하세요",
  }),
  text_columns: z.array(textColumnSchema).min(1, "컬럼을 최소 1개 이상 입력하세요"),
  cleanOptions: z.object({
    remove_english:       z.boolean(),
    remove_numbers:       z.boolean(),
    remove_special:       z.boolean(),
    remove_monosyllables: z.boolean(),
  }),
})

export type UploadVersionFormValues = z.infer<typeof versionSchema>