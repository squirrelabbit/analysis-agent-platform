import { MessageSquare, Plus } from "lucide-react";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { cn } from "@/lib/utils";
import { fmtDate } from "@/shared/utils/format";
import type { ChatThread } from "../models";

interface ThreadListProps {
  threads: ChatThread[];
  activeThreadId: string | null;
  isLoading: boolean;
  isComposing: boolean;
  onSelect: (threadId: string) => void;
  onNewThread: () => void;
}

export default function ThreadList({
  threads,
  activeThreadId,
  isLoading,
  isComposing,
  onSelect,
  onNewThread,
}: ThreadListProps) {
  return (
    <aside className="w-60 shrink-0 border-r border-zinc-100 bg-white flex flex-col overflow-hidden">
      <div className="flex items-center justify-between px-3 py-2 border-b border-zinc-100">
        <div className="flex items-center gap-1.5">
          <MessageSquare className="w-3.5 h-3.5 text-violet-500" />
          <span className="text-xs font-medium text-zinc-800">대화 이력</span>
        </div>
        <Button
          variant="ghost"
          size="sm"
          className="h-6 px-2 text-[11px] text-violet-600 hover:text-violet-700"
          onClick={onNewThread}
        >
          <Plus className="w-3 h-3 mr-0.5" />새 대화
        </Button>
      </div>

      <ScrollArea className="flex-1">
        {isLoading ? (
          <p className="text-[11px] text-zinc-400 px-3 py-4">불러오는 중…</p>
        ) : threads.length === 0 ? (
          <p className="text-[11px] text-zinc-400 px-3 py-4">
            저장된 대화가 없습니다.
          </p>
        ) : (
          <ul className="p-2 flex flex-col gap-1">
            {threads.map((t) => {
              const active = t.id === activeThreadId;
              return (
                <li key={t.id}>
                  <button
                    type="button"
                    onClick={() => onSelect(t.id)}
                    disabled={isComposing}
                    className={cn(
                      "w-full text-left rounded-md px-2.5 py-2 transition-colors disabled:opacity-50",
                      active
                        ? "bg-violet-50 border border-violet-200"
                        : "hover:bg-zinc-50 border border-transparent",
                    )}
                  >
                    <div className="text-xs font-medium text-zinc-800 truncate">
                      {t.title}
                    </div>
                    {t.lastMessage && (
                      <div className="text-[11px] text-zinc-500 truncate mt-0.5">
                        {t.lastMessage}
                      </div>
                    )}
                    <div className="flex items-center justify-between mt-1 text-[10px] text-zinc-400">
                      <span>{fmtDate(t.updatedAt)}</span>
                      <span>{t.messageCount}건</span>
                    </div>
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </ScrollArea>
    </aside>
  );
}
