import { ResultSection, StatCard, BarRow, DownloadButton } from "./Shared"
import { EmptyResult } from "../AnalysisResultTab"
import type { Artifact, BuildStage } from "@/features/dataset/types/datasetVersion"

export function CleanResult({
  stage,
  artifact,
  onDownload,
}: {
  stage: BuildStage
  artifact?: Artifact
  onDownload: (a: Artifact) => Promise<void>
}) {
  if (stage.status === "not_requested") return <EmptyResult message="파이프라인 탭에서 clean을 실행하세요" />
  if (!artifact) return <EmptyResult />

  const meta = artifact.metadata as {
    input_count?: number
    output_count?: number
    duplicate_removed?: number
    empty_removed?: number
    noise_normalized?: number
  }

  const input = meta.input_count ?? 0
  const output = meta.output_count ?? 0
  const removed = input - output

  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="정제 요약"
        action={<DownloadButton artifact={artifact} onDownload={onDownload} />}
      >
        <div className="grid grid-cols-3 gap-2 mb-4">
          <StatCard label="입력 행" value={input.toLocaleString()} unit="건" />
          <StatCard
            label="정제 후 행"
            value={output.toLocaleString()}
            unit="건"
            sub={`-${removed} 제거됨`}
            subColor="text-red-400"
          />
          <StatCard
            label="정제율"
            value={input > 0 ? ((output / input) * 100).toFixed(1) : "-"}
            unit="%"
          />
        </div>
        <div className="flex flex-col gap-3">
          <BarRow
            label="중복 제거"
            value={meta.duplicate_removed ?? 0}
            total={input}
            displayValue={`${meta.duplicate_removed ?? 0}건`}
            color="bg-red-300"
          />
          <BarRow
            label="빈 텍스트 제거"
            value={meta.empty_removed ?? 0}
            total={input}
            displayValue={`${meta.empty_removed ?? 0}건`}
            color="bg-red-300"
          />
          <BarRow
            label="노이즈 정규화"
            value={meta.noise_normalized ?? 0}
            total={input}
            displayValue={`${meta.noise_normalized ?? 0}건`}
            color="bg-amber-300"
          />
          <BarRow
            label="정상 처리"
            value={output}
            total={input}
            displayValue={`${output.toLocaleString()}건`}
            color="bg-emerald-400"
          />
        </div>
      </ResultSection>
    </div>
  )
}