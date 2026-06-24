import { Trash2 } from "lucide-react";
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

interface ThreadListItemProps {
  thread: ChatThread;
  active: boolean;
  deleting: boolean;
  disabled: boolean;
  onSelect: (threadId: string) => void;
  onDelete: (threadId: string) => void;
}

// 시안 「분석 채팅 - 보고서 패널」의 .hitem — 제목 + 날짜로 간결하게. 삭제는 시안엔
// 없지만 기능 유지를 위해 hover 시에만 드러나는 액션으로 둔다.
export default function ThreadListItem({
  thread,
  active,
  deleting,
  disabled,
  onSelect,
  onDelete,
}: ThreadListItemProps) {
  return (
    <div className="group relative">
      <button
        type="button"
        onClick={() => onSelect(thread.id)}
        disabled={disabled || deleting}
        className={cn(
          "block w-full rounded-[9px] py-2 pl-2.5 pr-8 text-left transition-colors disabled:opacity-50",
          active ? "bg-zinc-100" : "hover:bg-zinc-50",
        )}
      >
        <div
          className={cn(
            "truncate text-[13px] font-semibold",
            active ? "text-violet-700" : "text-zinc-800",
          )}
        >
          {thread.title}
        </div>
        <div className="mt-0.5 text-[11.5px] font-medium text-zinc-400">
          {fmtDate(thread.updatedAt)}
        </div>
      </button>
      <Dialog>
        <DialogTrigger asChild>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            disabled={disabled || deleting}
            aria-label="대화 삭제"
            className={cn(
              "absolute right-1.5 top-1.5 h-6 w-6 rounded-md text-zinc-400",
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
              <Button variant="destructive" onClick={() => onDelete(thread.id)}>
                삭제
              </Button>
            </DialogClose>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
