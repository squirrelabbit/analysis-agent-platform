import z from "zod";

export const datasetInfoSchema = z.object({
  name: z.string().min(1, "이름은 필수입니다"),
  description: z.string().min(1, "설명은 필수입니다"),
  dataType: z.enum(["structured", "unstructured"]),
});

export const datasetMetaSchema = z.object({
  subjectType: z.string().min(1, "분석 대상 유형은 필수입니다"),
  subjectName: z.string().min(1, "분석 대상명은 필수입니다"),
  subjectAliases: z.array(z.string()),
  recruitmentKeywords: z.array(z.string()),
  // aspect taxonomy (per-dataset). 빈 값이면 백엔드 default 사용.
  taxonomyId: z.string().optional(),
});

export type DatasetInfo = z.infer<typeof datasetInfoSchema>;
export type DatasetMeta = z.infer<typeof datasetMetaSchema>;
export type DatasetFormValues = DatasetInfo & {
  metadata: {
    docGenuineness?: DatasetMeta;
  };
};
