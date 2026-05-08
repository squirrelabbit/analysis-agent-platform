import { AlertTriangle } from "lucide-react"
import { ResultSection, StatCard, DownloadButton } from "./Shared"
import { EmptyResult } from "../AnalysisResultTab"
import type { Artifact, BuildStage } from "@/features/dataset/types/datasetVersion"

export function EmbeddingResult({
  stage,
  artifact,
  onDownload,
}: {
  stage: BuildStage
  artifact?: Artifact
  onDownload: (a: Artifact) => Promise<void>
}) {
  if (stage.status === "not_requested") return <EmptyResult message="파이프라인 탭에서 embedding을 실행하세요" />
  if (!artifact) return <EmptyResult />

  const meta = artifact.metadata as { text_column?: string; vector_dim?: number; vector_count?: number }
  const isStale = stage.status === "stale"

  return (
    <div className="flex flex-col gap-3">
      {isStale && (
        <div className="flex items-center gap-2 px-3.5 py-2.5 bg-amber-50 border border-amber-200 rounded-xl">
          <AlertTriangle className="w-3.5 h-3.5 text-amber-500 shrink-0" />
          <p className="text-xs text-amber-700">prepare output이 변경되어 재실행이 필요합니다</p>
        </div>
      )}
      <ResultSection
        title="임베딩 결과"
        action={!isStale ? <DownloadButton artifact={artifact} onDownload={onDownload} /> : undefined}
      >
        <div className="grid grid-cols-2 gap-2">
          <StatCard label="총 벡터 수" value={(meta.vector_count ?? 2048).toLocaleString()} unit="개" />
          <StatCard label="벡터 차원" value={meta.vector_dim ?? 1536} />
          <StatCard label="포맷" value={<span className="font-mono text-xs">{artifact.format}</span>} />
          <StatCard label="텍스트 컬럼" value={<span className="font-mono text-xs">{meta.text_column ?? "-"}</span>} />
        </div>
      </ResultSection>
    </div>
  )
}