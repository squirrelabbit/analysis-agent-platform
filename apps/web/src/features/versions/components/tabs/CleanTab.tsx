import { Database, Check, Minus, Percent, Clock } from "lucide-react";
import { cn } from "@/lib/utils";
import { StatCard } from "@/components/common/cards/StatCard";
import type { CleanBuild } from "../../models/build";
import { useBuildVersion } from "../../hooks/build.query";
import { formatSecond } from "@/shared/utils/format";

export default function CleanTab() {
  const { data } = useBuildVersion("clean") as { data: CleanBuild | undefined };
  const { summary, durationSeconds } = data || {};
  if (!summary) {
    return <p className="text-sm text-zinc-500">표시할 정제 요약이 없습니다.</p>;
  }

  const {
    inputRowCount,
    outputRowCount,
    keptCount,
    droppedCount,
    sourceInputCharCount,
    cleanReducedCharCount,
    cleanedInputCharCount,
    textColumn,
  } = summary;

  const cleanedPct = Math.round((outputRowCount / inputRowCount) * 100);
  const reductionPct = Math.round(
    (cleanReducedCharCount / sourceInputCharCount) * 100,
  );

  const bars: {
    label: string;
    value: number;
    max: number;
    color: string;
  }[] = [
    {
      label: "원본",
      value: sourceInputCharCount,
      max: sourceInputCharCount,
      color: "bg-blue-500",
    },
    {
      label: "정제 후",
      value: cleanedInputCharCount,
      max: sourceInputCharCount,
      color: "bg-emerald-500",
    },
    {
      label: "감소량",
      value: cleanReducedCharCount,
      max: sourceInputCharCount,
      color: "bg-amber-400",
    },
  ];

  return (
    <div className="space-y-5">
      {/* 메타 */}
      <div className="flex flex-wrap items-center gap-3 text-xs text-zinc-500">
        <span className="inline-flex items-center gap-1.5 font-medium">
          <Clock className="h-3.5 w-3.5 text-zinc-400" strokeWidth={1.8} />
          소요 시간
          <b className="font-bold text-zinc-800">{formatSecond(durationSeconds)}</b>
        </span>
      </div>

      {/* 처리 현황 */}
      <div>
        <p className="mb-3 text-[13px] font-bold text-zinc-600">처리 현황</p>
        <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
          <StatCard
            value={inputRowCount?.toLocaleString()}
            label="입력 행"
            icon={Database}
            tone="neutral"
          />
          <StatCard
            value={keptCount?.toLocaleString()}
            label="유지"
            icon={Check}
            tone="ok"
            valueColor="text-emerald-600"
          />
          <StatCard
            value={droppedCount?.toLocaleString()}
            label="제거"
            icon={Minus}
            tone={droppedCount > 0 ? "danger" : "muted"}
            valueColor={droppedCount > 0 ? "text-red-500" : undefined}
          />
          <StatCard
            value={`${cleanedPct}%`}
            label="정제율"
            icon={Percent}
            tone="blue"
            valueColor="text-blue-600"
          />
        </div>
      </div>

      {/* 문서 수 변화 */}
      <div>
        <p className="mb-3 text-[13px] font-bold text-zinc-600">문서 수 변화</p>
        <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
          <div className="text-[15px] font-bold text-zinc-900">
            원본 대비 정제 결과
          </div>
          <div className="mt-1 text-xs font-medium text-zinc-400">
            감소율 {reductionPct}% · 분석 컬럼: {textColumn}
          </div>
          <div className="mt-5 flex flex-col gap-4">
            {bars.map(({ label, value, max, color }) => (
              <div
                key={label}
                className="grid grid-cols-[64px_1fr_auto] items-center gap-3.5"
              >
                <span className="text-[13px] font-semibold text-zinc-500">
                  {label}
                </span>
                <div className="h-3 overflow-hidden rounded-full bg-zinc-100">
                  <div
                    className={cn(
                      "h-full rounded-full transition-all duration-700",
                      color,
                    )}
                    style={{ width: `${Math.round((value / max) * 100)}%` }}
                  />
                </div>
                <span className="min-w-21.5 text-right text-[13.5px] font-bold tabular-nums text-zinc-800">
                  {value?.toLocaleString()}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
