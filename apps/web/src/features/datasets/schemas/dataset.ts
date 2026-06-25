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
  // 행사별 추가 슬롯 (festival 통합 base, 2026-06-25). 진성 분류(doc_genuineness)와
  // 절 라벨링(clause_label)은 출력 스키마가 달라 task별로 분리한다(공용 금지).
  // 비우면 base 프롬프트만 사용. 채우면 base 끝에 append된다.
  genuinenessExtraInstructions: z.string().optional(),
  genuinenessExtraExamples: z.string().optional(),
  clauseExtraInstructions: z.string().optional(),
  clauseExtraExamples: z.string().optional(),
});

export type DatasetInfo = z.infer<typeof datasetInfoSchema>;
export type DatasetMeta = z.infer<typeof datasetMetaSchema>;
export type DatasetFormValues = DatasetInfo & {
  metadata: {
    docGenuineness?: DatasetMeta;
  };
};
