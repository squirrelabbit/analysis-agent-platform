import { apiClient } from "@/api/client";
import type { ReportSavedResultListResponseDto } from "../models";

// 보고서 탭은 보관함 조회/삭제만 담당한다. 저장(POST /saved_results)은 채팅에서 수행.
export const reportApi = {
  // GET 보관함 목록(project 스코프, 최신순). dataset_id로 특정 dataset만 필터(선택).
  listSavedResults: (projectId: string, datasetId?: string) =>
    apiClient
      .get<ReportSavedResultListResponseDto>(
        `/projects/${projectId}/saved_results`,
        { params: datasetId ? { dataset_id: datasetId } : undefined },
      )
      .then((r) => r.data),

  // DELETE 보관함 항목.
  deleteSavedResult: (projectId: string, resultId: string) =>
    apiClient
      .delete(`/projects/${projectId}/saved_results/${resultId}`)
      .then(() => undefined),
};
