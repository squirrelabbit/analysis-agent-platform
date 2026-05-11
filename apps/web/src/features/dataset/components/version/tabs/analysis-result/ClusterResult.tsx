import { ResultSection, StatCard, BarRow, DownloadButton } from "./Shared"
import { EmptyResult } from "../AnalysisResultTab"
import type { Artifact, BuildStage } from "@/features/dataset/types/datasetVersion"

export function ClusterResult({
  stage,
  artifact,
  onDownload,
}: {
  stage: BuildStage
  artifact?: Artifact
  onDownload: () => Promise<void>
}) {
  if (stage.status === "not_requested") return <EmptyResult message="파이프라인 탭에서 cluster를 실행하세요" />
  if (stage.status === "stale") return <EmptyResult message="embedding 재실행 후 cluster를 실행하세요" />
  if (!artifact) return <EmptyResult />

  const meta = artifact.metadata as {
    cluster_count?: number
    clusters?: { label: string; count: number }[]
  }

  const total = meta.clusters?.reduce((s, c) => s + c.count, 0) ?? 0

  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="클러스터 요약"
        action={<DownloadButton artifact={artifact} onDownload={onDownload} />}
      >
        <StatCard label="클러스터 수" value={meta.cluster_count ?? "-"} unit="개" />
      </ResultSection>

      {meta.clusters && meta.clusters.length > 0 && (
        <ResultSection title="클러스터별 분포">
          <div className="flex flex-col gap-3">
            {meta.clusters.map((c) => (
              <BarRow
                key={c.label}
                label={c.label}
                value={c.count}
                total={total}
                displayValue={`${c.count.toLocaleString()}건`}
                color="bg-indigo-400"
              />
            ))}
          </div>
        </ResultSection>
      )}
    </div>
  )
}