import { ResultSection, StatCard, DownloadButton } from "./Shared"
import { EmptyResult } from "../AnalysisResultTab"
import type { Artifact, SourceSummary } from "@/features/dataset/types/datasetVersion"

export function SourceResult({
  summary,
  artifact,
  onDownload,
}: {
  summary?: SourceSummary,
  artifact?: Artifact
  onDownload: () => Promise<void>
}) {
  if (!artifact || !summary) return <EmptyResult message="소스 파일 정보가 없습니다" />

  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="소스 파일 정보"
        action={artifact && <DownloadButton artifact={artifact} onDownload={onDownload} />}
      >
        <div className="grid grid-cols-3 gap-2">
          <StatCard label="레코드 수" value={summary.rowCount?.toLocaleString() ?? "-"} unit="건" />
          <StatCard label="컬럼 수" value={summary.columnCount ?? "-"} unit="개" />
          <StatCard label="포맷" value={summary.format} />
        </div>
      </ResultSection>
    </div>
  )
}