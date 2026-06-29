import { apiClient } from "@/api/client";
import type { BuildJobType } from "@/shared/types/common";
import type { BasicAnalysisResponse } from "../models/basicReport";

export interface BuildViewParams {
  limit?: number;
  offset?: number;
  aspect?: string;
  sentiment?: string;
  genuineness?: string;
  // 교차검증(verify) 검토 큐 필터 (ADR-026).
  disagreement?: boolean;
  needs_review?: boolean;
  // 키워드/절 부분일치 검색 (clause_keywords 등).
  q?: string;
  // clause_keywords item 집계 단위. "clause"면 절 중심({clause, keywords[]}).
  group?: string;
}

// 전처리 빌드(doc_genuineness/clause_label) 모델 선택지 (LLOA_MODELS allowlist).
export interface LloaModelOptionDto {
  model_id: string;
  label: string;
  default: boolean;
}

// 진성 분류 모델 비교 (2026-06-15). 두 버전 doc_id 1:1 비교 리포트.
export interface DocGenuinenessCompareSideDto {
  dataset_version_id: string;
  model?: string;
  model_display_name?: string;
  total: number;
}
export interface DocGenuinenessCompareDisagreementDto {
  doc_id: string;
  a_genuineness: string;
  a_reason?: string;
  b_genuineness: string;
  b_reason?: string;
  cleaned_text?: string;
  override_genuineness?: string;
}
export interface DocGenuinenessComparePatternDto {
  a_genuineness: string;
  b_genuineness: string;
  count: number;
}
export interface DocGenuinenessOverrideEvalDto {
  sample_count: number;
  a_correct: number;
  b_correct: number;
  a_accuracy: number;
  b_accuracy: number;
  leader: "a" | "b" | "tie";
}
export interface DocGenuinenessCompareDto {
  version_a: DocGenuinenessCompareSideDto;
  version_b: DocGenuinenessCompareSideDto;
  tiers: string[];
  compared: number;
  matched: number;
  agreement_rate: number;
  only_in_a: number;
  only_in_b: number;
  confusion: number[][];
  disagreements: DocGenuinenessCompareDisagreementDto[];
  disagreements_total: number;
  pagination?: { limit: number; offset: number; total: number };
  patterns: DocGenuinenessComparePatternDto[];
  override_eval?: DocGenuinenessOverrideEvalDto;
  unreviewed_disagreements: number;
  verdict_level: "ground_truth" | "agreement_only" | "review_needed";
}

// 한 버전에 보관된 모델별 진성 분류 결과(run).
export interface DocGenuinenessRunDto {
  model: string;
  model_display_name?: string;
  ref: string;
  prompt_version?: string;
  completed_at: string;
}

export const buildApi = {
  // 전역 read-only — 빌드 재실행 다이얼로그의 모델 select용. (2026-06-12)
  getLloaModelOptions: () =>
    apiClient
      .get<{ items: LloaModelOptionDto[] }>("/lloa_model_options")
      .then((r) => r.data.items),

  // 한 버전에 모델별로 누적된 진성 분류 결과 목록 (비교 선택지). (2026-06-15)
  getDocGenuinenessRuns: (projectId: string, datasetId: string, versionId: string) =>
    apiClient
      .get<{ dataset_version_id: string; items: DocGenuinenessRunDto[] }>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/doc_genuineness/runs`,
      )
      .then(({ data }) => data.items),

  // 진성 분류 모델 비교 — 한 버전 안의 두 모델을 doc_id 1:1 비교. (2026-06-15)
  compareDocGenuineness: (
    projectId: string,
    datasetId: string,
    versionId: string,
    modelA: string,
    modelB: string,
    params?: { limit?: number; offset?: number },
  ) =>
    apiClient
      .get<DocGenuinenessCompareDto>(
        `/projects/${projectId}/datasets/${datasetId}/doc_genuineness/compare`,
        { params: { version_id: versionId, model_a: modelA, model_b: modelB, ...params } },
      )
      .then(({ data }) => data),

  getBuildVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: BuildJobType,
    params?: BuildViewParams,
  ) =>
    apiClient
      .get(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`,
        { params },
      )
      .then(({ data }) => data),

  // 기초분석보고서 탭 — read-only 조회(report 저장 안 함). 템플릿 블록만 반환.
  // template_id 미지정 시 서버 기본 템플릿(unstructured_basic_v1).
  getBasicAnalysis: (
    projectId: string,
    datasetId: string,
    versionId: string,
    templateId?: string,
  ) =>
    apiClient
      .get<BasicAnalysisResponse>(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/basic_analysis`,
        { params: templateId ? { template_id: templateId } : undefined },
      )
      .then(({ data }) => data),

  runBuildVersion: <T>(
    projectId: string,
    datasetId: string,
    versionId: string,
    type: BuildJobType,
    req?: T
  ) =>
    apiClient
      .post(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}`, req,
      )
      .then(({ data }) => data),

  // 실행 중 build 중단(silverone 2026-06-29) — worker가 남은 doc 처리를 멈추고 거기까지
  // 결과를 보존한다. doc_genuineness / clause_label / clause_keywords만 지원.
  cancelBuildVersion: (
    projectId: string,
    datasetId: string,
    versionId: string,
    type: BuildJobType,
  ) =>
    apiClient
      .post(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/${type}/cancel`,
      )
      .then(({ data }) => data),

  // silverone 2026-06-11 — 진성 라벨 수동 보정. PATCH로 set, DELETE override로
  // 되돌리기. effective genuineness/reason은 GET doc_genuineness 응답에서 합성된다.
  setGenuinenessOverride: (
    projectId: string,
    datasetId: string,
    versionId: string,
    docId: string,
    req: { genuineness: string; reason?: string },
  ) =>
    apiClient
      .patch(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/doc_genuineness/${encodeURIComponent(docId)}`,
        req,
      )
      .then(({ data }) => data),

  deleteGenuinenessOverride: (
    projectId: string,
    datasetId: string,
    versionId: string,
    docId: string,
  ) =>
    apiClient
      .delete(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/doc_genuineness/${encodeURIComponent(docId)}/override`,
      )
      .then(() => undefined),

  // silverone 2026-06-11 — 절 aspect/sentiment 수동 보정. PATCH로 set, DELETE
  // override로 되돌리기. effective는 GET clause_label 응답에서 합성된다.
  setClauseLabelOverride: (
    projectId: string,
    datasetId: string,
    versionId: string,
    clauseId: string,
    req: { aspect?: string; sentiment?: string; reason?: string },
  ) =>
    apiClient
      .patch(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/clause_label/${encodeURIComponent(clauseId)}`,
        req,
      )
      .then(({ data }) => data),

  deleteClauseLabelOverride: (
    projectId: string,
    datasetId: string,
    versionId: string,
    clauseId: string,
  ) =>
    apiClient
      .delete(
        `/projects/${projectId}/datasets/${datasetId}/versions/${versionId}/clause_label/${encodeURIComponent(clauseId)}/override`,
      )
      .then(() => undefined),

  // 키워드 정제 사전 (silverone 2026-06-25) — dataset 단위. 제외=block, 대표어
  // 지정=synonym. 저장은 키워드 결과 overlay에 즉시 반영(원본 artifact 불변).
  listKeywordDictionary: (
    projectId: string,
    datasetId: string,
    includeInactive?: boolean,
  ) =>
    apiClient
      .get(`/projects/${projectId}/datasets/${datasetId}/keyword_dictionary`, {
        params: includeInactive ? { include_inactive: 1 } : undefined,
      })
      .then(({ data }) => data),

  setKeywordDictionaryRule: (
    projectId: string,
    datasetId: string,
    req: {
      rule_type: "block" | "synonym";
      source_term: string;
      target_term?: string;
      reason?: string;
    },
  ) =>
    apiClient
      .post(`/projects/${projectId}/datasets/${datasetId}/keyword_dictionary`, req)
      .then(({ data }) => data),

  setKeywordDictionaryRuleActive: (
    projectId: string,
    datasetId: string,
    ruleId: string,
    active: boolean,
    reason?: string,
  ) =>
    apiClient
      .post(
        `/projects/${projectId}/datasets/${datasetId}/keyword_dictionary/${encodeURIComponent(ruleId)}/${active ? "reactivate" : "deactivate"}`,
        { reason },
      )
      .then(({ data }) => data),

  // 규칙 완전 삭제(hard delete) — 해제(active=false)와 달리 목록에서 제거.
  // 변경 이력은 append-only로 남는다.
  deleteKeywordDictionaryRule: (
    projectId: string,
    datasetId: string,
    ruleId: string,
    reason?: string,
  ) =>
    apiClient
      .delete(
        `/projects/${projectId}/datasets/${datasetId}/keyword_dictionary/${encodeURIComponent(ruleId)}`,
        { params: reason ? { reason } : undefined },
      )
      .then(() => undefined),

  listKeywordDictionaryHistory: (projectId: string, datasetId: string) =>
    apiClient
      .get(`/projects/${projectId}/datasets/${datasetId}/keyword_dictionary/history`)
      .then(({ data }) => data),
};
