import { useEffect, useMemo, useRef, useState } from "react";
import { FileText, Send } from "lucide-react";
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
import { cn } from "@/lib/utils";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { createClientId } from "@/shared/utils/id";
import { useDatasets } from "@/features/datasets/hooks/dataset.query";
import { useAnalysisChat } from "../hooks/chat.mutation";
import { useChatThread } from "../hooks/chat.query";
import { useReportPanel } from "../hooks/useReportPanel";
import { useCreateReportFromPanel } from "../hooks/useCreateReportFromPanel";
import { useChatNav } from "../context/ChatNavContext";
import type { ChatMessage } from "../models";
import MessageBubble from "../components/MessageBubble";
import ChatToast from "../components/ChatToast";
import ReportPanel from "../components/ReportPanel";
import UnsavedReportGuard from "../components/UnsavedReportGuard";

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

  // 데이터셋·스레드 선택과 대화 이력은 공용 사이드바와 공유하므로 ChatNavContext에서
  // 가져온다. pendingTurn·입력·스크롤·보고서 패널은 이 화면 전용 상태로 남긴다.
  const nav = useChatNav();
  const activeDatasetId = nav.activeDatasetId;

  const [pendingTurn, setPendingTurn] = useState<PendingTurn | null>(null);
  const [input, setInput] = useState("");
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  const threadDetail = useChatThread(projectId, activeDatasetId, nav.threadId);
  const chat = useAnalysisChat(projectId, activeDatasetId);
  // 보고서 패널 — 결과 카드를 모아 제목·메모·순서 편집 후 한 번에 보고서 문서 생성.
  const panel = useReportPanel();
  const createReport = useCreateReportFromPanel(projectId);
  // 결과 액션 토스트 — 1.6초 후 자동 숨김.
  const [toast, setToast] = useState<{ msg: string; visible: boolean }>({
    msg: "",
    visible: false,
  });
  const toastTimer = useRef<ReturnType<typeof setTimeout> | null>(null);

  function showToast(msg: string) {
    setToast({ msg, visible: true });
    if (toastTimer.current) clearTimeout(toastTimer.current);
    toastTimer.current = setTimeout(
      () => setToast((t) => ({ ...t, visible: false })),
      1600,
    );
  }
  const isLoading = chat.isPending;

  const serverMessages = useMemo(
    () => threadDetail.data?.messages ?? [],
    [threadDetail.data],
  );

  // server messages를 source of truth로 두고, 분석 중인 turn만 pending으로 덧붙인다.
  // pending의 user는 처음엔 client uuid → 응답 받으면 server id로 교체되므로
  // refetch가 도착하면 baseIds.has로 자동 dedupe 된다.
  // 단 thread detail에는 run.status/error_message가 보존되지 않으므로
  // pendingTurn.assistant의 run 정보를 server message에 머지해 refetch 후에도
  // 같은 turn의 상태/에러가 사라지지 않게 한다 (백엔드 projection 추가 시
  // 머지 코드 제거 예정).
  const messages = useMemo(() => {
    if (!pendingTurn) return serverMessages;
    const baseIds = new Set(serverMessages.map((m) => m.id));
    const pendingAsst = pendingTurn.assistant;
    const hasPendingRun = !!pendingAsst && (pendingAsst.runStatus || pendingAsst.runError);
    const merged = hasPendingRun
      ? serverMessages.map((m) =>
          m.id === pendingAsst!.id
            ? {
                ...m,
                runStatus: m.runStatus ?? pendingAsst!.runStatus,
                runError: m.runError ?? pendingAsst!.runError,
              }
            : m,
        )
      : serverMessages;
    const extras: ChatMessage[] = [];
    if (!baseIds.has(pendingTurn.user.id)) extras.push(pendingTurn.user);
    if (pendingAsst && !baseIds.has(pendingAsst.id)) {
      extras.push(pendingAsst);
    }
    return extras.length ? [...merged, ...extras] : merged;
  }, [serverMessages, pendingTurn]);

  // 사용자 액션(send / thread select / new thread / dataset change)은 force로
  // 강제 하단 이동, messages 자체 변경은 near-bottom일 때만 따라간다 →
  // refetch/polling이 사용자를 위로 끌어내려 진행 중인 읽기를 방해하지 않음.
  const containerRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const scrollPolicyRef = useRef<"force" | "auto">("force");
  // handleSend가 새 서버 threadId로 승격할 때(null→id) threadId 변경 효과의 리셋이
  // 방금 만든 pendingTurn을 지우지 않도록 한 번만 건너뛰게 하는 플래그.
  const skipThreadResetRef = useRef(false);

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

  // 안정적인 컨텍스트 콜백만 추려 effect 의존성으로 쓴다(nav 객체 자체는 매 렌더
  // 새로 만들어져 의존하면 effect가 매번 도므로).
  const setComposing = nav.setComposing;

  // 스레드 전환·새 대화·삭제(컨텍스트가 threadId를 바꿈)에 반응해 진행 중 턴·에러를
  // 리셋하고 강제 하단 스크롤을 건다. 단 handleSend의 null→서버threadId 승격은
  // pendingTurn 보존을 위해 한 번 건너뛴다.
  // 보고서 패널은 일부러 리셋하지 않는다 — 여러 스레드의 결과를 한 보고서에 모을 수
  // 있도록 스레드 전환과 무관하게 유지한다(보고서 생성/전체 비우기/채팅 이탈 시 정리).
  useEffect(() => {
    if (skipThreadResetRef.current) {
      skipThreadResetRef.current = false;
      return;
    }
    setPendingTurn(null);
    setErrorMsg(null);
    scrollPolicyRef.current = "force";
  }, [nav.threadId]);

  // 데이터셋을 바꾸면(setDatasetId가 threadId도 해제) 입력창까지 비운다.
  useEffect(() => {
    // 사이드바에서 바뀌는 데이터셋에 맞춰 로컬 입력을 비우는 동기화 — 의도된 setState.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setInput("");
  }, [nav.datasetId]);

  // 전송 중 상태를 사이드바(대화 이력 비활성화)와 공유.
  useEffect(() => {
    setComposing(chat.isPending);
  }, [chat.isPending, setComposing]);

  async function handleSend() {
    const text = input.trim();
    if (!text || isLoading || !activeDatasetId) return;

    setErrorMsg(null);
    const localUser: ChatMessage = {
      id: createClientId(),
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
        threadId: nav.threadId ?? undefined,
      });
      // 새 대화면 서버 threadId로 승격(리셋 효과는 건너뛴다). 이어질문이면 변동 없음.
      if (res.threadId !== nav.threadId) {
        skipThreadResetRef.current = true;
        nav.setThreadId(res.threadId);
      }
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

  function handleAddToReport(msg: ChatMessage) {
    if (!msg.runId) return;
    // 추가 시점의 스레드를 출처로 보관 — 다른 스레드 결과도 한 보고서에 모을 수 있다.
    const added = panel.add(msg, nav.threadId ?? undefined);
    if (added) {
      showToast("보고서에 추가했습니다");
    } else {
      panel.openPanel();
      showToast("이미 보고서에 추가된 결과입니다");
    }
  }

  async function handleCreateReport() {
    setErrorMsg(null);
    try {
      await createReport.create({
        staged: panel.staged,
        reportTitle: panel.reportTitle,
        threadOf: panel.threadOf,
        messageOf: panel.messageOf,
        cardStateOf: panel.cardStateOf,
      });
      // 성공 시 에디터로 이동(navigate)되므로 별도 토스트 없음.
    } catch (err) {
      setErrorMsg(extractErrorMessage(err));
    }
  }

  const isDetailLoading = !!nav.threadId && threadDetail.isLoading;
  const hasContent = messages.length > 0;

  return (
    <div className="flex h-full overflow-hidden bg-zinc-50">
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
            <Select
              value={nav.activeDatasetId}
              onValueChange={nav.setDatasetId}
            >
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

            {/* 보고서 토글 — 추가된 결과가 1개 이상일 때 노출(시안 .rtoggle) */}
            {panel.count >= 1 && (
              <button
                type="button"
                onClick={panel.togglePanel}
                className={cn(
                  "inline-flex h-7 items-center gap-1.5 rounded-lg border px-2.5 text-[11px] font-bold transition",
                  panel.panelOpen
                    ? "border-zinc-900 bg-zinc-900 text-white"
                    : "border-violet-200 bg-violet-50 text-violet-700 hover:bg-violet-100",
                )}
              >
                <FileText className="h-3.5 w-3.5" />
                보고서
                <span
                  className={cn(
                    "grid h-4 min-w-4 place-items-center rounded-full px-1 text-[10px] font-extrabold",
                    panel.panelOpen
                      ? "bg-white/25 text-white"
                      : "bg-violet-600 text-white",
                  )}
                >
                  {panel.count}
                </span>
              </button>
            )}
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

            {messages.map((msg) => {
              const canReport = msg.role === "assistant" && !!msg.runId;
              return (
                <MessageBubble
                  key={msg.id}
                  message={msg}
                  onAddToReport={
                    canReport ? () => handleAddToReport(msg) : undefined
                  }
                  isAdded={canReport ? panel.isAdded(msg.runId!) : false}
                  title={canReport ? panel.titleFor(msg) : undefined}
                  onTitleChange={
                    canReport ? (v) => panel.setTitle(msg.runId!, v) : undefined
                  }
                  onToast={showToast}
                />
              );
            })}

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

      <ReportPanel
        panel={panel}
        onCreate={handleCreateReport}
        creating={createReport.isPending}
      />

      {/* 미저장 보고서 편집 경고 — 패널에 담긴 결과가 있고, 보고서 생성으로 인한
          의도된 이동이 아닐 때만 활성화한다. */}
      <UnsavedReportGuard
        active={panel.count > 0 && !createReport.isPending}
      />

      <ChatToast message={toast.msg} visible={toast.visible} />
    </div>
  );
}
