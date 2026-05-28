import { useEffect, useRef, useState } from "react";
import { Send } from "lucide-react";
import { ScrollArea } from "@/components/ui/scroll-area";
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
import type { ChatMessage } from "../models";
import MessageBubble from "../components/MessageBubble";

function extractErrorMessage(err: unknown): string {
  const detail = (
    err as { response?: { data?: { detail?: string } } }
  )?.response?.data?.detail;
  return detail || "분석 실행 중 오류가 발생했습니다.";
}

export function ChatPage() {
  const { projectId } = useProjectParams();
  const { data: datasets = [] } = useDatasets();

  const [datasetId, setDatasetId] = useState<string>("");
  const [messages, setMessages] = useState<ChatMessage[]>([]);
  const [threadId, setThreadId] = useState<string | null>(null);
  const [input, setInput] = useState("");
  const [errorMsg, setErrorMsg] = useState<string | null>(null);

  // 명시 선택이 없으면 첫 데이터셋을 기본값으로 (effect 없이 파생)
  const activeDatasetId = datasetId || datasets[0]?.id || "";

  const chat = useAnalysisChat(projectId, activeDatasetId);
  const isLoading = chat.isPending;

  const bottomRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages, isLoading]);

  // 데이터셋 변경 시 thread/대화 초기화 (thread는 dataset version에 고정)
  function handleDatasetChange(next: string) {
    setDatasetId(next);
    setThreadId(null);
    setMessages([]);
    setErrorMsg(null);
  }

  async function handleSend() {
    const text = input.trim();
    if (!text || isLoading || !activeDatasetId) return;

    setErrorMsg(null);
    const userMsg: ChatMessage = {
      id: crypto.randomUUID(),
      role: "user",
      content: text,
      createdAt: new Date().toISOString(),
    };
    setMessages((prev) => [...prev, userMsg]);
    setInput("");

    try {
      const res = await chat.mutateAsync({
        content: text,
        threadId: threadId ?? undefined,
      });
      setThreadId(res.threadId);
      if (res.assistantMessage) {
        setMessages((prev) => [...prev, res.assistantMessage!]);
      } else {
        setErrorMsg(res.errorMessage || "분석 결과를 가져오지 못했습니다.");
      }
    } catch (err) {
      setErrorMsg(extractErrorMessage(err));
    }
  }

  return (
    <div className="flex flex-col h-full overflow-hidden bg-zinc-50">
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
      <ScrollArea className="flex-1 px-5 py-4">
        <div className="flex flex-col gap-4 max-w-3xl mx-auto">
          {messages.length === 0 && !isLoading && (
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

          <div ref={bottomRef} />
        </div>
      </ScrollArea>

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
  );
}
