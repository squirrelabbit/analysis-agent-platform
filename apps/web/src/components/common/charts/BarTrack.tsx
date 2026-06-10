import { cn } from "@/lib/utils";

/**
 * 진행 막대 1개 (트랙 + 채움). 모든 가로 막대 차트의 공용 원자.
 * 높이는 className으로 지정한다 (예: "h-2", "h-2.5", "h-3"). 없으면 보이지 않는다.
 * 채움 색은 fillColor(hex/inline) 또는 fillClassName(Tailwind, 그라데이션 등)로 지정.
 */
export function BarTrack({
  percent,
  className,
  fillClassName,
  fillColor,
  fillStyle,
}: {
  percent: number; // 0-100
  className?: string;
  fillClassName?: string;
  fillColor?: string;
  fillStyle?: React.CSSProperties;
}) {
  return (
    <div className={cn("overflow-hidden rounded-full bg-zinc-100", className)}>
      <div
        className={cn("h-full rounded-full", fillClassName)}
        style={{
          width: `${Math.max(0, Math.min(100, percent))}%`,
          ...(fillColor ? { background: fillColor } : fillStyle),
        }}
      />
    </div>
  );
}
