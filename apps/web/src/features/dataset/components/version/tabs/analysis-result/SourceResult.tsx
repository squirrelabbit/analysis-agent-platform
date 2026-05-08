import { ResultSection, StatCard, DownloadButton } from "./Shared"
import { EmptyResult } from "../AnalysisResultTab"
import type { Artifact } from "@/features/dataset/types/datasetVersion"

export function SourceResult({
  artifact,
  onDownload,
}: {
  artifact?: Artifact
  onDownload: (a: Artifact) => Promise<void>
}) {
  if (!artifact) return <EmptyResult message="소스 파일 정보가 없습니다" />

  const meta = artifact.metadata as { record_count?: number; data_type?: string }

  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="소스 파일 정보"
        action={artifact && <DownloadButton artifact={artifact} onDownload={onDownload} />}
      >
        <div className="grid grid-cols-3 gap-2">
          <StatCard label="레코드 수" value={meta.record_count?.toLocaleString() ?? "-"} unit="건" />
          <StatCard label="데이터 타입" value={meta.data_type ?? "-"} />
          <StatCard label="포맷" value={artifact.format} />
        </div>
      </ResultSection>
    </div>
  )
}