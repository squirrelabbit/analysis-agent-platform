import type { BuildJobType } from "@/shared/types/common";

/**
 * build 단계(enum) → 화면 표시 한국어 라벨 단일 소스.
 * API는 enum 키(clean/doc_genuineness/clause_label)를 그대로 쓰고, 표시만 여기서 통일한다.
 * silverone 2026-06-05 — PipelineCard 영어 노출 + 화면별 표현 불일치 정리.
 */
export const BUILD_LABELS: Record<BuildJobType, string> = {
  source: "원본",
  clean: "데이터 정제",
  doc_genuineness: "진성 분석",
  clause_label: "절 라벨링",
  clause_keywords: "키워드 분석",
};

export const buildLabel = (type: BuildJobType): string =>
  BUILD_LABELS[type] ?? type;
