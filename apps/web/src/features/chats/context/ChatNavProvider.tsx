import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useLocation } from "react-router-dom";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useDatasets } from "@/features/datasets/hooks/dataset.query";
import { useChatThreads } from "../hooks/chat.query";
import {
  useDeleteChatThread,
  useRenameChatThread,
} from "../hooks/chat.mutation";
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
  // 새 대화 시작 신호(threadId만으로는 이미 null인 상태에서 다시 누른 걸 구분 못 함).
  const [newThreadNonce, setNewThreadNonce] = useState(0);

  const activeDatasetId = datasetId || datasets[0]?.id || "";

  // 채팅 화면에 "진입"할 때마다(다른 라우트→채팅) 항상 새 대화로 시작한다.
  // (threadId는 ProjectLayout 레벨 컨텍스트라 그대로 두면 이전에 보던 스레드를 끌고 온다.)
  // 채팅 안에서 스레드를 고르는 건 isChatRoute가 계속 true라 영향 없음.
  const wasChatRoute = useRef(false);
  useEffect(() => {
    if (isChatRoute && !wasChatRoute.current) setThreadId(null);
    wasChatRoute.current = isChatRoute;
  }, [isChatRoute]);

  // 채팅 라우트에서만 스레드 목록을 가져온다(데이터셋/보고서 화면에선 fetch 안 함).
  const threadsQuery = useChatThreads(
    projectId,
    isChatRoute ? activeDatasetId : "",
  );
  const deleteMutation = useDeleteChatThread(projectId, activeDatasetId);
  const renameMutation = useRenameChatThread(projectId, activeDatasetId);

  const value = useMemo<ChatNavValue>(() => {
    const deletingThreadId = deleteMutation.isPending
      ? (deleteMutation.variables ?? null)
      : null;
    const renamingThreadId = renameMutation.isPending
      ? (renameMutation.variables?.threadId ?? null)
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
        setNewThreadNonce((n) => n + 1);
      },
      newThreadNonce,
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
      renameThread: async (id, title) => {
        const next = title.trim();
        if (!next || renameMutation.isPending) return;
        const current = (threadsQuery.data ?? []).find((t) => t.id === id);
        if (current && current.title === next) return;
        try {
          await renameMutation.mutateAsync({ threadId: id, title: next });
        } catch (err) {
          // 수정 실패 시 목록은 유지된다(진단용 로그만 남긴다).
          console.error("대화 제목 수정 실패", err);
        }
      },
      threads: threadsQuery.data ?? [],
      threadsLoading: threadsQuery.isLoading,
      deletingThreadId,
      renamingThreadId,
      isComposing,
      setComposing,
      isChatRoute,
    };
  }, [
    datasetId,
    activeDatasetId,
    threadId,
    isComposing,
    newThreadNonce,
    threadsQuery.data,
    threadsQuery.isLoading,
    deleteMutation,
    renameMutation,
    isChatRoute,
  ]);

  return (
    <ChatNavContext.Provider value={value}>{children}</ChatNavContext.Provider>
  );
}
