import { MessageSquare, Plus, Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";
import { fmtDate } from "@/shared/utils/format";
import type { ChatThread } from "../models";

interface ThreadListProps {
  threads: ChatThread[];
  activeThreadId: string | null;
  isLoading: boolean;
  isComposing: boolean;
  deletingThreadId: string | null;
  onSelect: (threadId: string) => void;
  onNewThread: () => void;
  onDelete: (threadId: string) => void;
}

export default function ThreadList({
  threads,
  activeThreadId,
  isLoading,
  isComposing,
  deletingThreadId,
  onSelect,
  onNewThread,
  onDelete,
}: ThreadListProps) {
  return (
    <aside className="w-60 shrink-0 border-r border-zinc-100 bg-white flex flex-col min-h-0 overflow-hidden">
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

      <div className="flex-1 min-h-0 overflow-y-auto">
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
              const deleting = t.id === deletingThreadId;
              return (
                <li key={t.id} className="group relative">
                  <button
                    type="button"
                    onClick={() => onSelect(t.id)}
                    disabled={isComposing || deleting}
                    className={cn(
                      "w-full text-left rounded-md pl-2.5 pr-8 py-2 transition-colors disabled:opacity-50",
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
                  <Dialog>
                    <DialogTrigger asChild>
                      <Button
                        type="button"
                        variant="ghost"
                        size="icon"
                        disabled={isComposing || deleting}
                        aria-label="대화 삭제"
                        className={cn(
                          "absolute top-1.5 right-1.5 h-6 w-6 rounded text-zinc-400",
                          "opacity-0 group-hover:opacity-100 focus-visible:opacity-100",
                          "hover:bg-red-50 hover:text-red-500",
                          "disabled:opacity-30 disabled:pointer-events-none",
                        )}
                      >
                        <Trash2 className="w-3.5 h-3.5" />
                      </Button>
                    </DialogTrigger>
                    <DialogContent className="sm:max-w-sm">
                      <DialogHeader>
                        <DialogTitle>대화 삭제</DialogTitle>
                        <DialogDescription className="text-xs">
                          “{t.title}” 대화와 모든 메시지·실행 기록이 함께
                          삭제됩니다. 이 작업은 되돌릴 수 없습니다.
                        </DialogDescription>
                      </DialogHeader>
                      <DialogFooter className="flex gap-2">
                        <DialogClose asChild>
                          <Button variant="outline">취소</Button>
                        </DialogClose>
                        <DialogClose asChild>
                          <Button
                            variant="destructive"
                            onClick={() => onDelete(t.id)}
                          >
                            삭제
                          </Button>
                        </DialogClose>
                      </DialogFooter>
                    </DialogContent>
                  </Dialog>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </aside>
  );
}
