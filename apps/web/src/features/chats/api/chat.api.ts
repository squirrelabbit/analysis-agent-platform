import { apiClient } from "@/api/client";
import type {
  AnalysisThreadDetailDto,
  AnalysisThreadListResponseDto,
  AnalysisThreadMessageRequest,
  AnalysisThreadMessageResponseDto,
  AnalyzeUserQuestionRequest,
} from "../models";

// 분석은 sync 실행이라 기본 10s를 넘길 수 있어 별도 긴 타임아웃을 둔다.
const ANALYSIS_TIMEOUT_MS = 120_000;

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
};
