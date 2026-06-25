import { cn } from "@/lib/utils";
import { BarTrack } from "./BarTrack";

export type LegendDatum = {
  key: string;
  label: string;
  value: number;
  percent: number; // 0-100
  color: string; // hex
};

/**
 * 색 점 + 라벨 + 건수 + 퍼센트 + 진행 막대로 구성된 분포 범례.
 * 도넛 차트 옆 범례로 자주 쓴다 (percent가 막대 너비와 표시 % 둘 다를 구동).
 */
export function DistributionLegend({
  items,
  className,
  valueSuffix = "건",
}: {
  items: LegendDatum[];
  className?: string;
  valueSuffix?: string;
}) {
  return (
    <div className={cn("flex flex-col gap-4", className)}>
      {items.map((d) => (
        <div key={d.key}>
          <div className="flex items-center gap-2 text-[13px]">
            <span
              className="h-2.5 w-2.5 shrink-0 rounded-full"
              style={{ background: d.color }}
            />
            <span className="font-semibold text-zinc-600">{d.label}</span>
            <span className="ml-auto font-extrabold tabular-nums text-zinc-800">
              {d.value.toLocaleString()}
              {valueSuffix}
            </span>
            <span className="min-w-12 text-right font-semibold tabular-nums text-zinc-400">
              {d.percent}%
            </span>
          </div>
          <BarTrack className="mt-1.5 h-2" percent={d.percent} fillColor={d.color} />
        </div>
      ))}
    </div>
  );
}
