import { MetricCard } from "@/components/common/cards/MetricCard";
import type { CleanBuild } from "../../models/build";
import { useBuildVersion } from "../../hooks/build.query";

export default function CleanTab() {
  const { data } = useBuildVersion("clean") as { data: CleanBuild | undefined };
  const { summary } = data || {};
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
  return (
    <>
      <p className="text-xs font-medium text-zinc-400 uppercase tracking-widest mb-2">
        처리 현황
      </p>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-2">
        <MetricCard label="입력 행" value={inputRowCount?.toLocaleString()} />
        <MetricCard
          label="유지"
          value={keptCount?.toLocaleString()}
          valueColor="text-emerald-600"
        />
        <MetricCard
          label="제거"
          value={droppedCount?.toLocaleString()}
          valueColor={droppedCount > 0 ? "text-red-600" : "text-zinc-900"}
        />
        <MetricCard label="정제율" value={`${cleanedPct}%`} />
      </div>
      <div className="mt-3">
        <div className="rounded-xl border border-zinc-100 bg-white p-4 space-y-3">
          <p className="text-xs font-medium text-zinc-500 uppercase tracking-wider">
            문자 수 변화
          </p>
          {[
            {
              label: "원본",
              value: sourceInputCharCount,
              max: sourceInputCharCount,
              color: "bg-blue-300",
            },
            {
              label: "정제 후",
              value: cleanedInputCharCount,
              max: sourceInputCharCount,
              color: "bg-emerald-400",
            },
            {
              label: "감소량",
              value: cleanReducedCharCount,
              max: sourceInputCharCount,
              color: "bg-amber-400",
            },
          ].map(({ label, value, max, color }) => (
            <div key={label} className="flex items-center gap-3">
              <span className="w-14 text-xs text-zinc-500 shrink-0">
                {label}
              </span>
              <div className="flex-1 h-2 bg-zinc-100 rounded-full overflow-hidden">
                <div
                  className={`h-full rounded-full ${color} transition-all duration-700`}
                  style={{ width: `${Math.round((value / max) * 100)}%` }}
                />
              </div>
              <span className="w-14 text-right text-xs font-medium text-zinc-700">
                {value?.toLocaleString()}
              </span>
            </div>
          ))}
          <div className="pt-2 mt-1 border-t border-zinc-50 text-xs text-zinc-400">
            감소율{" "}
            <span className="font-medium text-zinc-600">{reductionPct}%</span>
            {" · "}분석 컬럼{" "}
            <span className="font-medium text-zinc-600">{textColumn}</span>
          </div>
        </div>
      </div>
    </>
  );
}
