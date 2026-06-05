import type { LucideIcon } from "lucide-react";
import { cn } from "@/lib/utils";

export type StatTone = "neutral" | "ok" | "danger" | "muted" | "blue";

const TONE_ICON: Record<StatTone, string> = {
  neutral: "bg-violet-50 text-violet-600",
  ok: "bg-emerald-50 text-emerald-600",
  danger: "bg-red-50 text-red-500",
  muted: "bg-zinc-100 text-zinc-400",
  blue: "bg-blue-50 text-blue-600",
};

/** 데이터 처리 현황 탭 공용 통계 카드 — 좌측 수치/라벨 + 우측 톤별 아이콘 박스 */
export function StatCard({
  value,
  label,
  icon: Icon,
  tone,
  valueColor,
}: {
  value: string | number;
  label: string;
  icon: LucideIcon;
  tone: StatTone;
  valueColor?: string;
}) {
  return (
    <div className="flex items-center justify-between gap-3 rounded-xl border border-zinc-100 bg-white px-4 py-4 shadow-sm">
      <div className="min-w-0">
        <div
          className={cn(
            "text-[26px] font-extrabold leading-none tracking-tight",
            valueColor ?? "text-zinc-900",
          )}
        >
          {value}
        </div>
        <div className="mt-2 truncate text-xs font-semibold text-zinc-500">
          {label}
        </div>
      </div>
      <div
        className={cn(
          "grid h-9.5 w-9.5 shrink-0 place-items-center rounded-[10px]",
          TONE_ICON[tone],
        )}
      >
        <Icon className="h-4.75 w-4.75" strokeWidth={1.8} />
      </div>
    </div>
  );
}
