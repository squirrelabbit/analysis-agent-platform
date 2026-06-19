import { useMemo, useState, type ReactNode } from "react";
import { useLocation } from "react-router-dom";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useDatasets } from "@/features/datasets/hooks/dataset.query";
import { useChatThreads } from "../hooks/chat.query";
import { useDeleteChatThread } from "../hooks/chat.mutation";
import { ChatNavContext, type ChatNavValue } from "./ChatNavContext";

export function ChatNavProvider({ children }: { children: ReactNode }) {
  const { projectId } = useProjectParams();
  const { pathname } = useLocation();
  const basePath = `/projects/${projectId}`;
  const isChatRoute =
    pathname === `${basePath}/chats` ||
    pathname.startsWith(`${basePath}/chats/`);

  const { data: datasets = [] } = useDatasets();
  const [datasetId, setDatasetIdState] = useState("");
  const [threadId, setThreadId] = useState<string | null>(null);
  const [isComposing, setComposing] = useState(false);

  const activeDatasetId = datasetId || datasets[0]?.id || "";

  // 채팅 라우트에서만 스레드 목록을 가져온다(데이터셋/보고서 화면에선 fetch 안 함).
  const threadsQuery = useChatThreads(
    projectId,
    isChatRoute ? activeDatasetId : "",
  );
  const deleteMutation = useDeleteChatThread(projectId, activeDatasetId);

  const value = useMemo<ChatNavValue>(() => {
    const deletingThreadId = deleteMutation.isPending
      ? (deleteMutation.variables ?? null)
      : null;
    return {
      datasetId,
      activeDatasetId,
      setDatasetId: (id) => {
        setDatasetIdState(id);
        setThreadId(null);
      },
      threadId,
      selectThread: (id) => {
        if (isComposing || id === threadId) return;
        setThreadId(id);
      },
      newThread: () => {
        if (isComposing) return;
        setThreadId(null);
      },
      setThreadId,
      deleteThread: async (id) => {
        if (isComposing || deleteMutation.isPending) return;
        try {
          await deleteMutation.mutateAsync(id);
          if (id === threadId) setThreadId(null);
        } catch (err) {
          // 삭제 실패 시 목록은 유지된다(진단용 로그만 남긴다).
          console.error("대화 삭제 실패", err);
        }
      },
      threads: threadsQuery.data ?? [],
      threadsLoading: threadsQuery.isLoading,
      deletingThreadId,
      isComposing,
      setComposing,
      isChatRoute,
    };
  }, [
    datasetId,
    activeDatasetId,
    threadId,
    isComposing,
    threadsQuery.data,
    threadsQuery.isLoading,
    deleteMutation,
    isChatRoute,
  ]);

  return (
    <ChatNavContext.Provider value={value}>{children}</ChatNavContext.Provider>
  );
}
