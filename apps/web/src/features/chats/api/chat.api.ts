import { apiClient } from "@/api/client";
import type {
  AnalysisThreadDetailDto,
  AnalysisThreadListResponseDto,
  AnalysisThreadMessageRequest,
  AnalysisThreadMessageResponseDto,
  AnalyzeUserQuestionRequest,
  ReportSavedResultCreateRequestDto,
  ReportSavedResultDto,
} from "../models";

// 분석은 sync 실행이라 전역 10s를 넘긴다 → 별도 per-request 타임아웃.
// 백엔드 worker 호출 timeout(PYTHON_AI_WORKER_HTTP_TIMEOUT_SEC, default 120s)
// 보다 약간 크게 둬서, 한도 초과 시 프론트가 먼저 끊지 않고 백엔드가 구조화된
// 에러를 반환하도록 한다. 운영에서 backend timeout을 크게 올리면 이 값도 함께
// 올려야 한다. (silverone 2026-06-04)
const ANALYSIS_TIMEOUT_MS = 130_000;

export const chatApi = {
  listThreads: (projectId: string, datasetId: string) =>
    apiClient
      .get<AnalysisThreadListResponseDto>(
        `/projects/${projectId}/datasets/${datasetId}/analysis_threads`,
      )
      .then((r) => r.data.items),

  getThread: (projectId: string, datasetId: string, threadId: string) =>
    apiClient
      .get<AnalysisThreadDetailDto>(
        `/projects/${projectId}/datasets/${datasetId}/analysis_threads/${threadId}`,
      )
      .then((r) => r.data),


  analyze: (
    projectId: string,
    datasetId: string,
    req: AnalyzeUserQuestionRequest,
  ) =>
    apiClient
      .post<AnalysisThreadMessageResponseDto>(
        `/projects/${projectId}/datasets/${datasetId}/analyze`,
        req,
        { timeout: ANALYSIS_TIMEOUT_MS },
      )
      .then((r) => r.data),

  sendMessage: (
    projectId: string,
    datasetId: string,
    threadId: string,
    req: AnalysisThreadMessageRequest,
  ) =>
    apiClient
      .post<AnalysisThreadMessageResponseDto>(
        `/projects/${projectId}/datasets/${datasetId}/analysis_threads/${threadId}/messages`,
        req,
        { timeout: ANALYSIS_TIMEOUT_MS },
      )
      .then((r) => r.data),

  deleteThread: (projectId: string, datasetId: string, threadId: string) =>
    apiClient
      .delete(
        `/projects/${projectId}/datasets/${datasetId}/analysis_threads/${threadId}`,
      )
      .then(() => undefined),

  // silverone 2026-06-10 — 분석 결과를 보고서 보관함에 저장(스냅샷). project
  // 스코프 endpoint(dataset/version은 run에서 유도).
  saveResult: (projectId: string, req: ReportSavedResultCreateRequestDto) =>
    apiClient
      .post<ReportSavedResultDto>(`/projects/${projectId}/saved_results`, req)
      .then((r) => r.data),
};
