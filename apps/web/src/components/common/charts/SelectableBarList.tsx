import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import { BarTrack } from "./BarTrack";

/**
 * 좌측 드릴다운 목록의 선택형 막대 행.
 * 라벨 | 막대 | 수치 | ChevronRight 4열 그리드. 선택 시 보라색 강조.
 * showBar=false면 막대 자리를 비운다 ("전체" 항목처럼 스케일 밖인 경우).
 */
export function SelectableBarRow({
  label,
  count,
  value,
  max,
  selected,
  onClick,
  showBar = true,
  labelWidth = 84,
  countMinWidth = "min-w-9",
  labelClassName,
}: {
  label: React.ReactNode;
  count: React.ReactNode; // 우측 수치 (이미 포맷된 값)
  value?: number; // 막대 비율 계산용
  max?: number;
  selected: boolean;
  onClick: () => void;
  showBar?: boolean;
  labelWidth?: number;
  countMinWidth?: string;
  labelClassName?: string;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{ gridTemplateColumns: `${labelWidth}px 1fr auto 16px` }}
      className={cn(
        "grid items-center gap-2.5 rounded-xl border-l-2 px-2 py-2 text-left transition-colors",
        selected
          ? "border-violet-500 bg-violet-50"
          : "border-transparent hover:cursor-pointer hover:bg-zinc-50",
      )}
    >
      <span
        className={cn(
          "truncate text-right text-xs font-semibold",
          selected ? "text-violet-700" : "text-zinc-600",
          labelClassName,
        )}
      >
        {label}
      </span>
      {showBar && value != null && max != null ? (
        <BarTrack
          className="h-2.5"
          percent={(value / max) * 100}
          fillClassName={cn(
            "bg-linear-to-r",
            selected
              ? "from-violet-600 to-violet-400"
              : "from-blue-500 to-blue-400",
          )}
        />
      ) : (
        <span aria-hidden />
      )}
      <span
        className={cn(
          "text-right text-xs font-bold tabular-nums text-zinc-800",
          countMinWidth,
        )}
      >
        {count}
      </span>
      <ChevronRight
        className={cn(
          "h-3.5 w-3.5 transition-colors",
          selected ? "text-violet-600" : "text-zinc-300",
        )}
      />
    </button>
  );
}
