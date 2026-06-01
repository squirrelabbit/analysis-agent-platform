import { useEffect, useMemo, useRef, useState } from "react";
import { Send } from "lucide-react";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useDatasets } from "@/features/datasets/hooks/dataset.query";
import { useAnalysisChat } from "../hooks/chat.mutation";
import { useChatThread, useChatThreads } from "../hooks/chat.query";
import type { ChatMessage } from "../models";
import MessageBubble from "../components/MessageBubble";
import ThreadList from "../components/ThreadList";

function extractErrorMessage(err: unknown): string {
  const detail = (
    err as { response?: { data?: { detail?: string } } }
  )?.response?.data?.detail;
  return detail || "분석 실행 중 오류가 발생했습니다.";
}

interface PendingTurn {
  user: ChatMessage;
  assistant?: ChatMessage;
}

export function ChatPage() {
  const { projectId } = useProjectParams();
  const { data: datasets = [] } = useDatasets();

  const [datasetId, setDatasetId] = useState<string>("");
  const [threadId, setThreadId] = useState<string | null>(null);
  const [pendingTurn, setPendingTurn] = useState<PendingTurn | null>(null);
  const [input, setInput] = useState("");
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const activeDatasetId = datasetId || datasets[0]?.id || "";

  const threadsQuery = useChatThreads(projectId, activeDatasetId);
  const threadDetail = useChatThread(projectId, activeDatasetId, threadId);
  const chat = useAnalysisChat(projectId, activeDatasetId);
  const isLoading = chat.isPending;

  const serverMessages = useMemo(
    () => threadDetail.data?.messages ?? [],
    [threadDetail.data],
  );

  // server messages를 source of truth로 두고, 분석 중인 turn만 pending으로 덧붙인다.
  // pending의 user는 처음엔 client uuid → 응답 받으면 server id로 교체되므로
  // refetch가 도착하면 baseIds.has로 자동 dedupe 된다.
  // 단 thread detail의 assistant message에는 plan이 보존되지 않으므로
  // (백엔드 작업 전) pendingTurn.assistant.plan은 server message에 머지해
  // 새 turn 직후엔 plan이 사라지지 않도록 한다.
  const messages = useMemo(() => {
    if (!pendingTurn) return serverMessages;
    const baseIds = new Set(serverMessages.map((m) => m.id));
    const pendingPlan = pendingTurn.assistant?.plan;
    const merged = pendingPlan
      ? serverMessages.map((m) =>
          m.id === pendingTurn.assistant?.id && !m.plan
            ? { ...m, plan: pendingPlan }
            : m,
        )
      : serverMessages;
    const extras: ChatMessage[] = [];
    if (!baseIds.has(pendingTurn.user.id)) extras.push(pendingTurn.user);
    if (
      pendingTurn.assistant &&
      !baseIds.has(pendingTurn.assistant.id)
    ) {
      extras.push(pendingTurn.assistant);
    }
    return extras.length ? [...merged, ...extras] : merged;
  }, [serverMessages, pendingTurn]);

  // 사용자 액션(send / thread select / new thread / dataset change)은 force로
  // 강제 하단 이동, messages 자체 변경은 near-bottom일 때만 따라간다 →
  // refetch/polling이 사용자를 위로 끌어내려 진행 중인 읽기를 방해하지 않음.
  const containerRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const scrollPolicyRef = useRef<"force" | "auto">("force");

  useEffect(() => {
    const el = containerRef.current;
    const content = contentRef.current;
    if (!el || !content) return;
    // 메시지가 아직 안 들어왔으면 force 플래그를 소비하지 않고 보존 →
    // thread fetch 완료 후 첫 messages 변경 때 비로소 끝으로 이동.
    if (messages.length === 0) return;
    const distance = el.scrollHeight - el.scrollTop - el.clientHeight;
    const force = scrollPolicyRef.current === "force";
    if (!force && distance > 80) return;
    // 직접 assignment가 가장 robust — behavior 옵션 없는 대신 layout 시점에
    // 정확하다.
    const scrollEnd = () => {
      el.scrollTop = el.scrollHeight;
    };
    scrollEnd();
    // force일 때 차트/표 등 비동기 layout으로 scrollHeight가 늦게 늘어나도
    // 끝에 붙어있도록 짧은 윈도우 동안 자식 크기 변화에 반응한다.
    // 윈도우가 끝나면 그제서야 force를 auto로 reset → 그 사이에 일어나는
    // 후속 effect도 force로 다시 끝으로 보낸다 (첫 scrollTo가 실패해도 복구).
    if (!force) return;
    const observer = new ResizeObserver(scrollEnd);
    observer.observe(content);
    const timer = setTimeout(() => {
      observer.disconnect();
      scrollPolicyRef.current = "auto";
    }, 600);
    return () => {
      observer.disconnect();
      clearTimeout(timer);
    };
  }, [messages, isLoading]);

  function resetThreadState() {
    setThreadId(null);
    setPendingTurn(null);
    setErrorMsg(null);
    setInput("");
    scrollPolicyRef.current = "force";
  }

  function handleDatasetChange(next: string) {
    setDatasetId(next);
    resetThreadState();
  }

  function handleSelectThread(next: string) {
    if (next === threadId || isLoading) return;
    setThreadId(next);
    setPendingTurn(null);
    setErrorMsg(null);
    scrollPolicyRef.current = "force";
  }

  function handleNewThread() {
    if (isLoading) return;
    resetThreadState();
  }

  async function handleSend() {
    const text = input.trim();
    if (!text || isLoading || !activeDatasetId) return;

    setErrorMsg(null);
    const localUser: ChatMessage = {
      id: crypto.randomUUID(),
      role: "user",
      content: text,
      createdAt: new Date().toISOString(),
    };
    setPendingTurn({ user: localUser });
    setInput("");
    scrollPolicyRef.current = "force";

    try {
      const res = await chat.mutateAsync({
        content: text,
        threadId: threadId ?? undefined,
      });
      setThreadId(res.threadId);
      // server id로 교체 → 곧이어 들어올 detail refetch와 자연스럽게 dedupe.
      setPendingTurn({
        user: res.userMessage ?? localUser,
        assistant: res.assistantMessage,
      });
      if (!res.assistantMessage) {
        setErrorMsg(res.errorMessage || "분석 결과를 가져오지 못했습니다.");
      }
    } catch (err) {
      setErrorMsg(extractErrorMessage(err));
    }
  }

  const threads = threadsQuery.data ?? [];
  const isDetailLoading = !!threadId && threadDetail.isLoading;
  const hasContent = messages.length > 0;

  return (
    <div className="flex h-full overflow-hidden bg-zinc-50">
      <ThreadList
        threads={threads}
        activeThreadId={threadId}
        isLoading={threadsQuery.isLoading}
        isComposing={isLoading}
        onSelect={handleSelectThread}
        onNewThread={handleNewThread}
      />

      <div className="flex flex-col flex-1 min-h-0 overflow-hidden">
        {/* 헤더 */}
        <div className="flex items-center justify-between px-5 py-3 bg-white border-b border-zinc-100 shrink-0">
          <div>
            <h2 className="text-sm font-medium text-zinc-900">분석 채팅</h2>
            <p className="text-[11px] text-zinc-400 mt-0.5">
              선택한 데이터셋의 활성 버전 기준으로 분석합니다.
            </p>
          </div>
          <div className="flex items-center gap-2">
            <span className="text-[11px] text-zinc-500">데이터셋</span>
            <Select value={activeDatasetId} onValueChange={handleDatasetChange}>
              <SelectTrigger className="w-48 h-7 text-[11px]">
                <SelectValue placeholder="데이터셋 선택" />
              </SelectTrigger>
              <SelectContent>
                {datasets.map((d) => (
                  <SelectItem key={d.id} value={d.id}>
                    {d.name}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
          </div>
        </div>

        {/* 메시지 목록 */}
        <div
          ref={containerRef}
          className="flex-1 min-h-0 overflow-y-auto px-5 py-4"
        >
          <div
            ref={contentRef}
            className="flex flex-col gap-4 max-w-3xl mx-auto"
          >
            {isDetailLoading && !hasContent && (
              <p className="text-center text-xs text-zinc-400 py-8">
                대화를 불러오는 중…
              </p>
            )}

            {!isDetailLoading && !hasContent && !isLoading && (
              <p className="text-center text-xs text-zinc-400 py-8">
                {activeDatasetId
                  ? "분석 질문을 입력해 대화를 시작하세요."
                  : "먼저 데이터셋을 선택하세요."}
              </p>
            )}

            {messages.map((msg) => (
              <MessageBubble key={msg.id} message={msg} />
            ))}

            {isLoading && (
              <div className="flex gap-2.5 items-start">
                <Avatar className="h-7 w-7 shrink-0 mt-0.5">
                  <AvatarFallback className="bg-violet-100 text-violet-700 text-[10px]">
                    AI
                  </AvatarFallback>
                </Avatar>
                <div className="bg-white border border-zinc-100 rounded-2xl rounded-tl-sm px-4 py-3">
                  <div className="flex gap-1 items-center h-3">
                    {[0, 1, 2].map((i) => (
                      <span
                        key={i}
                        className="w-1.5 h-1.5 rounded-full bg-zinc-400 animate-bounce"
                        style={{ animationDelay: `${i * 0.15}s` }}
                      />
                    ))}
                  </div>
                </div>
              </div>
            )}

            {errorMsg && (
              <div className="mx-auto rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-600">
                {errorMsg}
              </div>
            )}
          </div>
        </div>

        {/* 입력창 */}
        <div className="border-t border-zinc-100 bg-white px-5 py-3 shrink-0">
          <div className="flex gap-2 max-w-3xl mx-auto">
            <Input
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && handleSend()}
              placeholder={
                activeDatasetId
                  ? "분석 질문을 입력하세요..."
                  : "데이터셋을 선택하세요"
              }
              className="flex-1 h-9 text-sm"
              disabled={isLoading || !activeDatasetId}
            />
            <Button
              variant="secondary"
              size="sm"
              onClick={handleSend}
              disabled={!input.trim() || isLoading || !activeDatasetId}
              className="px-4 h-9"
            >
              <Send className="w-3.5 h-3.5" />
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}
