import { ResultSection, StatCard, BarRow, DownloadButton } from "./Shared";
import { EmptyResult } from "../AnalysisResultTab";
import type {
  Artifact,
  BuildStage,
} from "@/features/dataset/types/datasetVersion";
import { Item, ItemHeader, ItemTitle } from "@/components/ui/item";
import { Badge } from "@/components/ui/badge";

export function CleanResult({
  stage,
  artifact,
  onDownload,
}: {
  stage: BuildStage;
  artifact?: Artifact;
  onDownload: () => Promise<void>;
}) {
  const { status, summary } = stage;
  if (status === "not_requested" || !summary)
    return <EmptyResult message="파이프라인 탭에서 clean을 실행하세요" />;
  if (!artifact) return <EmptyResult />;

  const meta = artifact.metadata as {
    input_count?: number;
    output_count?: number;
    duplicate_removed?: number;
    empty_removed?: number;
    noise_normalized?: number;
  };

  const {
    input_row_count,
    dropped_count,
    output_row_count,
    kept_count,
    skipped_row_count,
    preprocess_options,
    clean_reduced_char_count,
    source_input_char_count,
    cleaned_input_char_count,
  } = summary;

  const {
    remove_english,
    remove_monosyllables,
    remove_numbers,
    remove_special,
  } = preprocess_options;

  const OPTIONS = [
    { label: "영문 제거", value: remove_english },
    { label: "숫자 제거", value: remove_monosyllables },
    { label: "특수문자 제거", value: remove_numbers },
    { label: "단음절 제거", value: remove_special },
  ];
  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="정제 요약"
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
        <div className="grid grid-cols-2 gap-2 mb-4">
          <StatCard
            label="원본 문자 수"
            value={source_input_char_count.toLocaleString()}
            unit="건"
          />
          <StatCard
            label="정제 후 문자 수"
            value={cleaned_input_char_count.toLocaleString()}
            unit="건"
            sub={`-${clean_reduced_char_count.toLocaleString()} 감소`}
            subColor="text-red-400"
          />
        </div>
        <div className="grid grid-cols-2 gap-2 mb-4">
          {OPTIONS.map((op) => (
            <Item variant="outline" size={"xs"}>
              <ItemHeader>
                <ItemTitle className="text-xs">{op.label}</ItemTitle>
                <Badge variant={"secondary"}>{op.value ? "on" : "off"}</Badge>
              </ItemHeader>
            </Item>
          ))}
        </div>
        <div className="flex flex-col gap-3">
          {/* <BarRow
            label="중복 제거"
            value={meta.duplicate_removed ?? 0}
            total={input_row_count}
            displayValue={`${meta.duplicate_removed ?? 0}건`}
            color="bg-red-300"
          />
          <BarRow
            label="빈 텍스트 제거"
            value={meta.empty_removed ?? 0}
            total={input_row_count}
            displayValue={`${meta.empty_removed ?? 0}건`}
            color="bg-red-300"
          /> */}
          <BarRow
            label="스킵"
            value={meta.noise_normalized ?? 0}
            total={input_row_count}
            displayValue={`${skipped_row_count.toLocaleString() ?? 0}건`}
            color="bg-amber-300"
          />
          <BarRow
            label="정상 처리"
            value={kept_count}
            total={input_row_count}
            displayValue={`${kept_count.toLocaleString()}건`}
            color="bg-emerald-400"
          />
        </div>
      </ResultSection>
    </div>
  );
}
