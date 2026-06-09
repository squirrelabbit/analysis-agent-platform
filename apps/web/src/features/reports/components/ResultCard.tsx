import { BarChart3, Check, MessageSquare, MoreHorizontal, Table2 } from "lucide-react";
import { cn } from "@/lib/utils";
import { ResultPreview } from "./ResultPreview";
import type { ReportResult } from "../models/model";

export function ResultCard({
  result,
  selected,
  onToggle,
}: {
  result: ReportResult;
  selected: boolean;
  onToggle: () => void;
}) {
  const isChart = result.kind === "chart";
  return (
    <div
      onClick={onToggle}
      className={cn(
        "group cursor-pointer overflow-hidden rounded-2xl border bg-white shadow-sm transition-all hover:-translate-y-0.5 hover:shadow-md",
        selected
          ? "border-violet-300 ring-2 ring-violet-100"
          : "border-zinc-100",
      )}
    >
      {/* top: 체크박스 + 타입 뱃지 + 메뉴 */}
      <div className="flex items-center gap-2 px-3.5 pb-2.5 pt-3.5">
        <span
          className={cn(
            "grid h-5 w-5 place-items-center rounded-md border-2 transition-colors",
            selected
              ? "border-violet-600 bg-violet-600"
              : "border-zinc-300 bg-white",
          )}
        >
          <Check
            className={cn(
              "h-3 w-3 text-white transition-opacity",
              selected ? "opacity-100" : "opacity-0",
            )}
            strokeWidth={3}
          />
        </span>
        <span
          className={cn(
            "inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[11.5px] font-bold",
            isChart
              ? "bg-violet-50 text-violet-700"
              : "bg-blue-50 text-blue-600",
          )}
        >
          {isChart ? (
            <BarChart3 className="h-3 w-3" />
          ) : (
            <Table2 className="h-3 w-3" />
          )}
          {isChart ? "차트" : "표"}
        </span>
        <button
          onClick={(e) => e.stopPropagation()}
          className="ml-auto grid h-6 w-6 place-items-center rounded-md text-zinc-300 transition-colors hover:bg-zinc-100 hover:text-zinc-500"
        >
          <MoreHorizontal className="h-4 w-4" />
        </button>
      </div>

      {/* preview */}
      <div
        className={cn(
          "mx-3.5 grid h-33 place-items-center overflow-hidden rounded-[10px] border border-zinc-100 bg-zinc-50/70",
          result.kind === "table" ? "items-stretch p-0" : "p-2.5",
        )}
      >
        <ResultPreview result={result} />
      </div>

      {/* body */}
      <div className="px-3.5 pb-3.5 pt-3">
        <div className="text-sm font-bold leading-snug text-zinc-900">
          {result.title}
        </div>
        <div className="mt-1.5 flex items-center gap-1.5 text-xs text-zinc-400">
          <MessageSquare className="h-3 w-3 shrink-0" />
          <span className="truncate">{result.chat}</span>
        </div>
        <div className="mt-2 flex items-center gap-2 text-[11px] font-medium text-zinc-400">
          {result.time}
          <span className="h-0.75 w-0.75 rounded-full bg-zinc-300" />
          {result.rows}
        </div>
      </div>
    </div>
  );
}
