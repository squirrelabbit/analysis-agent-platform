import { cn } from "@/lib/utils";
import { BarTrack } from "./BarTrack";

/**
 * 라벨 | 막대 | 값 한 줄. CleanTab의 비교 막대처럼 단순 가로 막대 목록에 사용.
 * fillClassName으로 막대 색을 지정한다 (예: "bg-emerald-500").
 */
export function BarRow({
  label,
  value,
  max,
  fillClassName,
  valueSuffix = "",
  labelWidth = 64,
  valueMinWidth = "min-w-21.5",
}: {
  label: React.ReactNode;
  value: number;
  max: number;
  fillClassName: string;
  valueSuffix?: string;
  labelWidth?: number;
  valueMinWidth?: string;
}) {
  return (
    <div
      className="grid items-center gap-3.5"
      style={{ gridTemplateColumns: `${labelWidth}px 1fr auto` }}
    >
      <span className="text-[13px] font-semibold text-zinc-500">{label}</span>
      <BarTrack
        className="h-3"
        percent={(value / max) * 100}
        fillClassName={cn("transition-all duration-700", fillClassName)}
      />
      <span
        className={cn(
          "text-right text-[13.5px] font-bold tabular-nums text-zinc-800",
          valueMinWidth,
        )}
      >
        {value?.toLocaleString()}
        {valueSuffix}
      </span>
    </div>
  );
}
