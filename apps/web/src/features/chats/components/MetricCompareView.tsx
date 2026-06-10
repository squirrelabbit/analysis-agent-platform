import type { ChatMetric } from "../models";
import { CHANGE_UP_TEXT as INCREASE_COLOR, CHANGE_DOWN_TEXT as DECREASE_COLOR } from "../models/theme";

// 증감 방향 색은 앱 canonical(증가=초록/감소=빨강, theme.ts)을 따른다 — diverging 차트와 통일.

function fmtCount(v: number | null, unit: string): string {
  if (v === null) return "—";
  return `${v.toLocaleString()}${unit}`;
}

// 두 기간 total 비교(1행) — "전 N건 → 후 M건, Δ +20건 (+25.6%)" 카드.
export default function MetricCompareView({ metric }: { metric: ChatMetric }) {
  const { aValue, bValue, deltaValue, deltaRate, unit } = metric;
  const up = (deltaValue ?? 0) >= 0;
  const deltaColor = up ? INCREASE_COLOR : DECREASE_COLOR;
  const sign = up ? "+" : "";
  const deltaText = deltaValue === null ? "—" : `${sign}${deltaValue.toLocaleString()}${unit}`;
  const rateText = deltaRate === null ? null : `${sign}${deltaRate.toFixed(1)}%`;

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white p-4">
      <div className="flex items-center gap-3">
        <div className="flex flex-col">
          <span className="text-xs text-zinc-400">이전</span>
          <span className="text-lg font-semibold text-zinc-700">{fmtCount(aValue, unit)}</span>
        </div>
        <span className="text-zinc-300 text-lg">→</span>
        <div className="flex flex-col">
          <span className="text-xs text-zinc-400">이후</span>
          <span className="text-lg font-semibold text-zinc-900">{fmtCount(bValue, unit)}</span>
        </div>
        <div className="ml-auto flex flex-col items-end">
          <span className="text-xs text-zinc-400">변화</span>
          <span className={`text-lg font-bold ${deltaColor}`}>
            {deltaText}
            {rateText && <span className="ml-1 text-sm font-medium">({rateText})</span>}
          </span>
        </div>
      </div>
    </div>
  );
}
