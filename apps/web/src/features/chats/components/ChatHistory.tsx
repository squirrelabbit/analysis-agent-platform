import { Plus } from "lucide-react";
import { useChatNav } from "../context/ChatNavContext";
import ThreadListItem from "./ThreadListItem";

// 공용 사이드바의 "채팅" 항목 아래에 붙는 대화 이력 섹션(시안 「분석 채팅 - 보고서 패널」의
// .hist 영역). 상태/핸들러는 모두 ChatNavContext에서 가져온다.
export default function ChatHistory() {
  const nav = useChatNav();

  return (
    <div className="flex min-h-0 flex-1 flex-col border-t border-zinc-100 pt-3">
      <div className="flex items-center justify-between px-2.5 pb-2">
        <span className="text-[11px] font-extrabold uppercase tracking-wider text-zinc-400">
          대화 이력
        </span>
        <button
          type="button"
          onClick={nav.newThread}
          disabled={nav.isComposing}
          aria-label="새 대화"
          title="새 대화"
          className="grid h-6 w-6 place-items-center rounded-md text-zinc-400 transition-colors hover:bg-zinc-100 hover:text-zinc-700 disabled:opacity-40"
        >
          <Plus className="h-[15px] w-[15px]" />
        </button>
      </div>

      <div className="flex min-h-0 flex-1 flex-col gap-px overflow-y-auto pr-0.5">
        {nav.threadsLoading ? (
          <p className="px-2.5 py-3 text-[11px] text-zinc-400">불러오는 중…</p>
        ) : nav.threads.length === 0 ? (
          <p className="px-2.5 py-3 text-[11px] text-zinc-400">
            저장된 대화가 없습니다.
          </p>
        ) : (
          nav.threads.map((t) => (
            <ThreadListItem
              key={t.id}
              thread={t}
              active={t.id === nav.threadId}
              deleting={t.id === nav.deletingThreadId}
              renaming={t.id === nav.renamingThreadId}
              disabled={nav.isComposing}
              onSelect={nav.selectThread}
              onDelete={nav.deleteThread}
              onRename={nav.renameThread}
            />
          ))
        )}
      </div>
    </div>
  );
}
