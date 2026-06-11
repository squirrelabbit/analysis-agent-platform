import { useMutation, useQueryClient } from "@tanstack/react-query";
import { reportApi } from "../api/report.api";
import { reportKeys } from "../api/report.key";

// 보고서 보관함 항목 삭제. 성공 시 보관함 목록 전체 무효화(필터별 캐시 모두 갱신).
export const useDeleteSavedResult = (projectId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (resultId: string) =>
      reportApi.deleteSavedResult(projectId, resultId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: reportKeys.savedResults() });
    },
  });
};
