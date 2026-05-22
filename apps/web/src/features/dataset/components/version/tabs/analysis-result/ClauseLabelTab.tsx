import type { ClauseLabelSummary } from "@/features/dataset/types/datasetVersion";
import { BarRow, ResultSection, StatCard } from "./Shared";

export function ClauseLabelTab({ summary }: { summary: ClauseLabelSummary }) {
  const {
    clauseCount,
    inputRowCount,
    model,
    promptVersion,
    sentimentCounts,
    // includeGenuineness,
    // parseFailures,
    // processedDocCount,
    // skippedByFilter,
    // skippedEmpty,
    // totalCompletionTokens,
    // totalPromptTokens,
  } = summary;

  const { negative, neutral, positive, mixed } = sentimentCounts;

  const DONUT_SEGMENTS = [
    {
      label: "긍정",
      count: positive,
      color: "#22C55E",
      pct: inputRowCount > 0 ? positive / inputRowCount : 0,
    },
    {
      label: "부정",
      count: negative,
      color: "#F87171",
      pct: inputRowCount > 0 ? negative / inputRowCount : 0,
    },
    {
      label: "중립",
      count: neutral,
      color: "#A1A1AA",
      pct: inputRowCount > 0 ? neutral / inputRowCount : 0,
    },
    {
      label: "긍부정 혼합",
      count: mixed,
      color: "#FBBF24",
      pct: inputRowCount > 0 ? mixed / inputRowCount : 0,
    },
  ];

  // SVG 도넛 계산
  const R = 28;
  const C = 2 * Math.PI * R;
  let offset = 0;
  const segments = DONUT_SEGMENTS.map((seg) => {
    const dash = seg.pct * C;
    const result = { ...seg, dash, offset };
    offset += dash;
    return result;
  });

  return (
    <div className="flex flex-col gap-3">
      <ResultSection title="감성 분포">
        {/* 도넛 + 범례 */}
        <div className="flex items-center gap-5 mb-4">
          <svg width="80" height="80" viewBox="0 0 80 80" className="shrink-0">
            <circle
              cx="40"
              cy="40"
              r={R}
              fill="none"
              stroke="#F3F4F6"
              strokeWidth="14"
            />
            {segments.map((seg, i) => (
              <circle
                key={i}
                cx="40"
                cy="40"
                r={R}
                fill="none"
                stroke={seg.color}
                strokeWidth="14"
                strokeDasharray={`${seg.dash} ${C}`}
                strokeDashoffset={-seg.offset + C / 4}
                transform="rotate(-90 40 40)"
              />
            ))}
            <text
              x="40"
              y="44"
              textAnchor="middle"
              fontSize="11"
              fontWeight="600"
              fill="#18181B"
            >
              {clauseCount.toLocaleString()}
            </text>
          </svg>
          <div className="flex flex-col gap-2 flex-1">
            {DONUT_SEGMENTS.map((seg) => (
              <div key={seg.label} className="flex items-center gap-2 text-xs">
                <div
                  className="w-2 h-2 rounded-sm shrink-0"
                  style={{ background: seg.color }}
                />
                <span className="text-zinc-500 flex-1">{seg.label}</span>
                <span className="font-medium text-zinc-700">
                  {(seg.pct * 100).toFixed(0)}%
                </span>
                <span className="text-zinc-400">
                  {seg.count.toLocaleString()}건
                </span>
              </div>
            ))}
          </div>
        </div>
        <div className="grid grid-cols-2 gap-2 mb-3">
          <StatCard
            label="모델"
            value={<span className="font-mono text-xs">{model ?? "-"}</span>}
          />
          <StatCard
            label="프롬프트 버전"
            value={
              <span className="font-mono text-xs">{promptVersion ?? "-"}</span>
            }
          />
        </div>
      </ResultSection>

      <ResultSection title="감성별 분포">
        <div className="flex flex-col gap-3">
          {DONUT_SEGMENTS.map((seg) => (
            <BarRow
              key={seg.label}
              label={seg.label}
              value={seg.count}
              total={clauseCount}
              displayValue={`${seg.count.toLocaleString()}건 (${(seg.pct * 100).toFixed(0)}%)`}
              color={
                seg.label === "긍정"
                  ? "bg-emerald-400"
                  : seg.label === "부정"
                    ? "bg-red-300"
                    : seg.label === "중립"
                      ? "bg-zinc-300"
                      : "bg-amber-400"
              }
            />
          ))}
        </div>
      </ResultSection>
    </div>
  );
}
