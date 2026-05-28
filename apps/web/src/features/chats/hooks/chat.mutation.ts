import { useMutation } from "@tanstack/react-query";
import { chatApi } from "../api/chat.api";
import { mapAnalyzeResponse } from "../models";

// sync 실행: analyze(첫 질문) / messages(이어질문) 모두 분석 완료 후 결과를 바로 반환한다.
export const useAnalysisChat = (projectId: string, datasetId: string) =>
  useMutation({
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
  });
