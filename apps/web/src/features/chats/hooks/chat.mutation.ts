import { useMutation, useQueryClient } from "@tanstack/react-query";
import { chatApi } from "../api/chat.api";
import { chatKeys } from "../api/chat.key";
import { mapAnalyzeResponse } from "../models";

// sync 실행: analyze(첫 질문) / messages(이어질문) 모두 분석 완료 후 결과를 바로 반환한다.
export const useAnalysisChat = (projectId: string, datasetId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({
      content,
      threadId,
    }: {
      content: string;
      threadId?: string;
    }) => {
      const request = threadId
        ? chatApi.sendMessage(projectId, datasetId, threadId, { content })
        : chatApi.analyze(projectId, datasetId, { user_question: content });
      return request.then(mapAnalyzeResponse);
    },
    onSuccess: (result) => {
      // thread 목록(last_message/updated_at) 갱신, 열린 thread 상세도 동기화.
      queryClient.invalidateQueries({
        queryKey: chatKeys.threadList(projectId, datasetId),
      });
      queryClient.invalidateQueries({
        queryKey: chatKeys.threadDetail(projectId, datasetId, result.threadId),
      });
    },
  });
};
