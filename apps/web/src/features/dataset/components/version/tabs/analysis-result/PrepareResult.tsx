import { ResultSection, StatCard, DownloadButton } from "./Shared";
import { EmptyResult } from "../AnalysisResultTab";
import type {
  Artifact,
  BuildStage,
} from "@/features/dataset/types/datasetVersion";

export function PrepareResult({
  stage,
  artifact,
  onDownload,
}: {
  stage: BuildStage;
  artifact?: Artifact;
  onDownload: () => Promise<void>;
}) {
  const { status, summary, promptVersion, model } = stage;
  if (status === "not_requested" || !summary)
    return <EmptyResult message="파이프라인 탭에서 prepare를 실행하세요" />;
  if (!artifact) return <EmptyResult />;

  const {
    input_row_count,
    dropped_count,
    output_row_count,
    prepare_batch_size,
    text_column,
  } = summary;
  
  const meta = artifact.metadata as {
    prepare_notes?: string[];
    prepared_text_column?: string;
    raw_text_column?: string;
    skipped_rows?: number;
  };

  // const skipped = meta.skipped_rows ?? 0

  // const skipReasons = [
  //   { id: "#102",  reason: "텍스트 길이 초과 (max token)" },
  //   { id: "#347",  reason: "언어 감지 실패" },
  //   { id: "#891",  reason: "텍스트 길이 초과 (max token)" },
  //   { id: "#1204", reason: "빈 normalized_text 반환" },
  //   { id: "#1589", reason: "언어 감지 실패" },
  // ]

  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="전처리 요약"
        action={<DownloadButton artifact={artifact} onDownload={onDownload} />}
      >
        <div className="grid grid-cols-3 gap-2 mb-4">
          <StatCard
            label="입력 행"
            value={input_row_count.toLocaleString()}
            unit="건"
          />
          <StatCard
            label="정제 후 행"
            value={output_row_count.toLocaleString()}
            unit="건"
            sub={`-${dropped_count} 제거됨`}
            subColor="text-red-400"
          />
          <StatCard
            label="정제율"
            value={
              input_row_count > 0
                ? ((output_row_count / input_row_count) * 100).toFixed(1)
                : "-"
            }
            unit="%"
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
          <StatCard label="배치 크기" value={prepare_batch_size ?? "-"} />
          {/* <StatCard
            label="스킵"
            value={skipped}
            unit="건"
            sub={skipped > 0 ? "처리 불가" : undefined}
            subColor="text-red-400"
          /> */}
        </div>
        <div className="grid grid-cols-2 gap-2">
          <StatCard
            label="정제 컬럼"
            value={
              <span className="font-mono text-xs">{text_column ?? "-"}</span>
            }
          />
          <StatCard
            label="출력 컬럼"
            value={
              <span className="font-mono text-xs">
                {meta.prepared_text_column ?? "-"}
              </span>
            }
          />
        </div>
      </ResultSection>

      {/* {skipped > 0 && (
        <ResultSection title="스킵된 행" action={<span className="text-xs text-zinc-400">{skipped}건</span>}>
          <div className="flex flex-col gap-1.5 max-h-40 overflow-y-auto">
            {skipReasons.map((r) => (
              <div key={r.id} className="flex items-center gap-2 px-2.5 py-1.5 bg-zinc-50 rounded-lg">
                <span className="text-[10px] text-zinc-400 font-mono w-10 shrink-0">{r.id}</span>
                <span className="text-xs text-zinc-600 flex-1">{r.reason}</span>
                <Badge variant="outline" className="text-[9px] h-4 px-1.5 bg-red-50 text-red-600 border-red-200">
                  skip
                </Badge>
              </div>
            ))}
          </div>
        </ResultSection>
      )} */}
    </div>
  );
}
