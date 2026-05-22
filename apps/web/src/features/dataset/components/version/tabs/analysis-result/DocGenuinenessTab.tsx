import type { DocGenuinenessSummary } from "@/features/dataset/types/datasetVersion";
import { ResultSection, StatCard } from "./Shared";

export function DocGenuinenessTab({
  summary,
}: {
  summary: DocGenuinenessSummary;
}) {
  const {
    inputRowCount,
    model,
    parseFailures,
    processedRowCount,
    promptVersion,
    // tierCounts,
    totalCompletionTokens,
    totalPromptTokens,
  } = summary;

  return (
    <ResultSection title="전처리 요약">
      <div className="grid grid-cols-3 gap-2 mb-4">
        <StatCard
          label="입력 행"
          value={inputRowCount.toLocaleString()}
          unit="건"
        />
        <StatCard
          label="실패 행"
          value={parseFailures.toLocaleString()}
          unit="건"
        />
        <StatCard
          label="분석 행"
          value={processedRowCount.toLocaleString()}
          unit="건"
        />
      </div>
      <div className="grid grid-cols-2 gap-2 mb-4">
        <StatCard
          label="전체 완료 토큰 수"
          value={totalCompletionTokens?.toLocaleString()}
          unit="개"
        />
        <StatCard
          label="전체 프롬프트 토큰 수"
          value={totalPromptTokens?.toLocaleString()}
          unit="개"
        />
      </div>
      <div className="grid grid-cols-3 gap-2 mb-3">
        <StatCard
          label="모델"
          value={<span className="font-mono text-xs">{model ?? "-"}</span>}
        />
        <StatCard
          label="프롬프트 버전"
          value={
            <span className="font-mono text-xs">{promptVersion ?? ""}</span>
          }
        />
      </div>
    </ResultSection>
  );
}
