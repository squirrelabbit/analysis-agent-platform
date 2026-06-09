import { FileText } from "lucide-react";
import { cn } from "@/lib/utils";

// 항목 선택 시 하단에 떠오르는 액션 바.
export function SelectionBar({
  count,
  onClear,
  onCreate,
}: {
  count: number;
  onClear: () => void;
  onCreate: () => void;
}) {
  return (
    <div
      className={cn(
        "fixed bottom-6 left-1/2 z-40 flex items-center gap-3.5 rounded-2xl bg-zinc-900 py-2.5 pl-5 pr-3 text-white shadow-2xl transition-all duration-300",
        count > 0
          ? "-translate-x-1/2 translate-y-0 opacity-100"
          : "pointer-events-none translate-y-32 -translate-x-1/2 opacity-0",
      )}
    >
      <div className="text-sm font-bold whitespace-nowrap">
        <span className="text-violet-300">{count}</span>개 선택됨
      </div>
      <span className="h-5 w-px bg-white/15" />
      <button
        onClick={onClear}
        className="rounded-lg px-2 py-1.5 text-[13px] font-semibold text-white/75 transition-colors hover:bg-white/10 hover:text-white"
      >
        선택 해제
      </button>
      <button
        onClick={onCreate}
        className="inline-flex h-9 items-center gap-1.5 rounded-xl bg-white px-4 text-[13.5px] font-bold text-zinc-900 transition-transform hover:-translate-y-0.5"
      >
        <FileText className="h-4 w-4" />
        보고서 만들기
      </button>
    </div>
  );
}
