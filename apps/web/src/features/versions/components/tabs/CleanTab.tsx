import { Database, Check, Minus, Percent, type LucideIcon } from "lucide-react";
import { StatCard, type StatTone } from "@/components/common/cards/StatCard";
import { BarRow } from "@/components/common/charts";
import type { CleanBuild } from "../../models/build";
import { useBuildVersion } from "../../hooks/build.query";
import {
  BuildMetaBar,
  BuildRunningBanner,
  BuildTabEmpty,
  BuildTabLoading,
  isBuildRunning,
} from "../BuildStatusMeta";

export default function CleanTab() {
  const { data, isLoading } = useBuildVersion("clean") as {
    data: CleanBuild | undefined;
    isLoading: boolean;
  };
  const { summary, status, progress, durationSeconds } = data || {};
  if (isLoading) return <BuildTabLoading />;
  if (!summary) {
    return isBuildRunning(status) ? (
      <BuildRunningBanner
        status={status}
        progress={progress}
        hasPrevious={false}
      />
    ) : (
      <BuildTabEmpty type="clean" status={status} />
    );
  }

  const {
    inputRowCount,
    outputRowCount,
    keptCount,
    dedupedCount,
    sourceInputCharCount,
    cleanReducedCharCount,
    cleanedInputCharCount,
    textColumn,
  } = summary;

  const cleanedPct = Math.round((outputRowCount / inputRowCount) * 100);
  const reductionPct = Math.round(
    (cleanReducedCharCount / sourceInputCharCount) * 100,
  );

  // 처리 현황 카드 (값은 표시 문자열로 미리 포맷 — 건수는 천단위, 정제율은 %).
  const stats: {
    value: string;
    label: string;
    icon: LucideIcon;
    tone: StatTone;
    valueColor?: string;
  }[] = [
    {
      value: inputRowCount.toLocaleString(),
      label: "입력 행",
      icon: Database,
      tone: "neutral",
    },
    {
      value: keptCount.toLocaleString(),
      label: "유지",
      icon: Check,
      tone: "ok",
      valueColor: "text-emerald-600",
    },
    {
      value: dedupedCount.toLocaleString(),
      label: "중복제거",
      icon: Minus,
      tone: dedupedCount > 0 ? "danger" : "muted",
      valueColor: dedupedCount > 0 ? "text-red-500" : undefined,
    },
    {
      value: `${cleanedPct}%`,
      label: "정제율",
      icon: Percent,
      tone: "blue",
      valueColor: "text-blue-600",
    },
  ];

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
      <BuildMetaBar status={status} durationSeconds={durationSeconds} />

      <BuildRunningBanner status={status} progress={progress} hasPrevious />

      {/* 처리 현황 */}
      <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
        {stats.map((s) => (
          <StatCard
            key={s.label}
            value={s.value}
            label={s.label}
            icon={s.icon}
            tone={s.tone}
            valueColor={s.valueColor}
          />
        ))}
      </div>

      {/* 문서 수 변화 */}
      <div className="rounded-2xl border border-zinc-100 bg-white p-5 shadow-sm">
        <div className="text-[15px] font-bold text-zinc-900">
          원본 대비 정제 결과
        </div>
        <div className="mt-1 text-xs font-medium text-zinc-400">
          감소율 {reductionPct}% · 분석 컬럼: {textColumn}
        </div>
        <div className="mt-5 flex flex-col gap-4">
          {bars.map(({ label, value, max, color }) => (
            <BarRow
              key={label}
              label={label}
              value={value}
              max={max}
              fillClassName={color}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
