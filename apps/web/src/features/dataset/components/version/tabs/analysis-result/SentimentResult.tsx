import { ResultSection, StatCard, BarRow, DownloadButton } from "./Shared"
import { EmptyResult } from "../AnalysisResultTab"
import type { Artifact, BuildStage } from "@/features/dataset/types/datasetVersion"

export function SentimentResult({
  stage,
  artifact,
  onDownload,
}: {
  stage: BuildStage
  artifact?: Artifact
  onDownload: (a: Artifact) => Promise<void>
}) {
  if (stage.status === "not_requested") return <EmptyResult message="파이프라인 탭에서 sentiment를 실행하세요" />
  if (!artifact) return <EmptyResult />

  const meta = artifact.metadata as {
    sentiment_usage?: {
      model: string
      request_count: number
      estimated_cost_usd: number
      cost_estimation_status: string
    }
    sentiment_label_column?: string
    sentiment_confidence_column?: string
    // 실제 분포는 API 별도 제공
  }

  const usage = meta.sentiment_usage
  const total = usage?.request_count ?? 0

  // mock 분포 — 실제는 API에서 제공
  const dist = { positive: 1065, negative: 573, neutral: 410 }

  const DONUT_SEGMENTS = [
    { label: "긍정", count: dist.positive, color: "#22C55E", pct: total > 0 ? dist.positive / total : 0 },
    { label: "부정", count: dist.negative, color: "#F87171", pct: total > 0 ? dist.negative / total : 0 },
    { label: "중립", count: dist.neutral,  color: "#A1A1AA", pct: total > 0 ? dist.neutral / total  : 0 },
  ]

  // SVG 도넛 계산
  const R = 28
  const C = 2 * Math.PI * R
  let offset = 0
  const segments = DONUT_SEGMENTS.map(seg => {
    const dash = seg.pct * C
    const result = { ...seg, dash, offset }
    offset += dash
    return result
  })

  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="감성 분포"
        action={<DownloadButton artifact={artifact} onDownload={onDownload} />}
      >
        {/* 도넛 + 범례 */}
        <div className="flex items-center gap-5 mb-4">
          <svg width="80" height="80" viewBox="0 0 80 80" className="shrink-0">
            <circle cx="40" cy="40" r={R} fill="none" stroke="#F3F4F6" strokeWidth="14" />
            {segments.map((seg, i) => (
              <circle
                key={i}
                cx="40" cy="40" r={R}
                fill="none"
                stroke={seg.color}
                strokeWidth="14"
                strokeDasharray={`${seg.dash} ${C}`}
                strokeDashoffset={-seg.offset + C / 4}
                transform="rotate(-90 40 40)"
              />
            ))}
            <text x="40" y="44" textAnchor="middle" fontSize="11" fontWeight="600" fill="#18181B">
              {total.toLocaleString()}
            </text>
          </svg>
          <div className="flex flex-col gap-2 flex-1">
            {DONUT_SEGMENTS.map((seg) => (
              <div key={seg.label} className="flex items-center gap-2 text-xs">
                <div className="w-2 h-2 rounded-sm shrink-0" style={{ background: seg.color }} />
                <span className="text-zinc-500 flex-1">{seg.label}</span>
                <span className="font-medium text-zinc-700">{(seg.pct * 100).toFixed(0)}%</span>
                <span className="text-zinc-400">{seg.count.toLocaleString()}건</span>
              </div>
            ))}
          </div>
        </div>

        <div className="grid grid-cols-2 gap-2">
          <StatCard label="모델" value={<span className="font-mono text-xs">{usage?.model ?? "-"}</span>} />
          <StatCard
            label="비용"
            value={usage?.cost_estimation_status === "free_fallback" ? "무료" : `$${usage?.estimated_cost_usd ?? 0}`}
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
              total={total}
              displayValue={`${seg.count.toLocaleString()}건 (${(seg.pct * 100).toFixed(0)}%)`}
              color={seg.label === "긍정" ? "bg-emerald-400" : seg.label === "부정" ? "bg-red-300" : "bg-zinc-300"}
            />
          ))}
        </div>
      </ResultSection>
    </div>
  )
}