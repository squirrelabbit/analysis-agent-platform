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

// silverone 2026-06-10 — 분석 결과를 보고서 보관함에 저장. project 스코프라
// dataset/version은 백엔드가 run에서 유도한다. 보고서 탭 API 연동은 아직
// 없으므로 별도 invalidate는 하지 않는다(저장 성공 여부만 사용).
export const useSaveReportResult = (projectId: string) =>
  useMutation({
    mutationFn: ({ runId, threadId }: { runId: string; threadId?: string }) =>
      chatApi.saveResult(projectId, {
        run_id: runId,
        thread_id: threadId,
      }),
  });

export const useDeleteChatThread = (projectId: string, datasetId: string) => {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (threadId: string) =>
      chatApi.deleteThread(projectId, datasetId, threadId),
    onSuccess: (_, threadId) => {
      queryClient.removeQueries({
        queryKey: chatKeys.threadDetail(projectId, datasetId, threadId),
      });
      queryClient.invalidateQueries({
        queryKey: chatKeys.threadList(projectId, datasetId),
      });
    },
  });
};
