import { apiClient } from "@/api/client";
import type { BuildJobType } from "@/shared/types/common";

export interface BuildViewParams {
  limit?: number;
  offset?: number;
  aspect?: string;
  sentiment?: string;
  genuineness?: string;
  // 키워드/절 부분일치 검색 (clause_keywords 등).
  q?: string;
}

export const buildApi = {
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
};
