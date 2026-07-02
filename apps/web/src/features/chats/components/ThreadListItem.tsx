import { useEffect, useRef, useState } from "react";
import { Pencil, Trash2 } from "lucide-react";
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

const TITLE_MAX = 80; // 백엔드 analysisTitle()과 동일 — 80 rune에서 잘림.

interface ThreadListItemProps {
  thread: ChatThread;
  active: boolean;
  deleting: boolean;
  renaming: boolean;
  disabled: boolean;
  onSelect: (threadId: string) => void;
  onDelete: (threadId: string) => void;
  onRename: (threadId: string, title: string) => void;
}

// 시안 「분석 채팅 - 보고서 패널」의 .hitem — 제목 + 날짜로 간결하게. 제목 수정(#28)과
// 삭제는 hover 시에만 드러나는 액션. 제목 수정은 인라인 input(Enter/blur 저장, Esc 취소).
export default function ThreadListItem({
  thread,
  active,
  deleting,
  renaming,
  disabled,
  onSelect,
  onDelete,
  onRename,
}: ThreadListItemProps) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState(thread.title);
  const inputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (editing) {
      inputRef.current?.focus();
      inputRef.current?.select();
    }
  }, [editing]);

  const startEdit = () => {
    if (disabled || deleting || renaming) return;
    setDraft(thread.title);
    setEditing(true);
  };

  const commit = () => {
    if (!editing) return;
    setEditing(false);
    const next = draft.trim();
    if (next && next !== thread.title) onRename(thread.id, next);
  };

  const cancel = () => {
    setEditing(false);
    setDraft(thread.title);
  };

  if (editing) {
    return (
      <div className="relative">
        <input
          ref={inputRef}
          value={draft}
          maxLength={TITLE_MAX}
          onChange={(e) => setDraft(e.target.value)}
          onBlur={commit}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              commit();
            } else if (e.key === "Escape") {
              e.preventDefault();
              cancel();
            }
          }}
          className={cn(
            "block w-full rounded-[9px] border border-violet-300 bg-white py-2 pl-2.5 pr-2.5",
            "text-[13px] font-semibold text-zinc-800 outline-none",
            "focus:border-violet-400 focus:ring-2 focus:ring-violet-100",
          )}
          aria-label="대화 제목 수정"
        />
      </div>
    );
  }

  return (
    <div className="group relative">
      <button
        type="button"
        onClick={() => onSelect(thread.id)}
        disabled={disabled || deleting || renaming}
        className={cn(
          "block w-full rounded-[9px] py-2 pl-2.5 pr-14 text-left transition-colors disabled:opacity-50",
          active ? "bg-zinc-100" : "hover:bg-zinc-50",
        )}
      >
        <div
          className={cn(
            "truncate text-[13px] font-semibold",
            active ? "text-violet-700" : "text-zinc-800",
          )}
        >
          {renaming ? "수정 중…" : thread.title}
        </div>
        <div className="mt-0.5 text-[11.5px] font-medium text-zinc-400">
          {fmtDate(thread.updatedAt)}
        </div>
      </button>

      <div className="absolute right-1.5 top-1.5 flex gap-0.5">
        <Button
          type="button"
          variant="ghost"
          size="icon"
          onClick={startEdit}
          disabled={disabled || deleting || renaming}
          aria-label="대화 제목 수정"
          className={cn(
            "h-6 w-6 rounded-md text-zinc-400",
            "opacity-0 group-hover:opacity-100 focus-visible:opacity-100",
            "hover:bg-violet-50 hover:text-violet-500",
            "disabled:pointer-events-none disabled:opacity-30",
          )}
        >
          <Pencil className="h-3.5 w-3.5" />
        </Button>
        <Dialog>
          <DialogTrigger asChild>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              disabled={disabled || deleting || renaming}
              aria-label="대화 삭제"
              className={cn(
                "h-6 w-6 rounded-md text-zinc-400",
                "opacity-0 group-hover:opacity-100 focus-visible:opacity-100",
                "hover:bg-red-50 hover:text-red-500",
                "disabled:pointer-events-none disabled:opacity-30",
              )}
            >
              <Trash2 className="h-3.5 w-3.5" />
            </Button>
          </DialogTrigger>
          <DialogContent className="sm:max-w-sm">
            <DialogHeader>
              <DialogTitle>대화 삭제</DialogTitle>
              <DialogDescription className="text-xs">
                “{thread.title}” 대화와 모든 메시지·실행 기록이 함께 삭제됩니다. 이
                작업은 되돌릴 수 없습니다.
              </DialogDescription>
            </DialogHeader>
            <DialogFooter className="flex gap-2">
              <DialogClose asChild>
                <Button variant="outline">취소</Button>
              </DialogClose>
              <DialogClose asChild>
                <Button
                  variant="destructive"
                  onClick={() => onDelete(thread.id)}
                >
                  삭제
                </Button>
              </DialogClose>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </div>
  );
}
