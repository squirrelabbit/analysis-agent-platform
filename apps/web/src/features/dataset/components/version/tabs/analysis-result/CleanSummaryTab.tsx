import {
  ResultSection,
  StatCard,
  BarRow,
  // DownloadButton,
} from "./Shared";
import type { CleanSummary } from "@/features/dataset/types/datasetVersion";
import { Item, ItemHeader, ItemTitle } from "@/components/ui/item";
import { Badge } from "@/components/ui/badge";

export function CleanSummaryTab({
  summary,
  // onDownload,
}: {
  summary: CleanSummary;
  onDownload?: () => Promise<void>;
}) {
  const {
    inputRowCount,
    outputRowCount,
    keptCount,
    droppedCount,
    preprocessOptions,
    sourceInputCharCount,
    cleanedInputCharCount,
    cleanReducedCharCount,
  } = summary;

  console.log(summary);
  const { removeEnglish, removeMonosyllables, removeNumbers, removeSpecial } =
    preprocessOptions;

  const OPTIONS = [
    { label: "영문 제거", value: removeEnglish },
    { label: "숫자 제거", value: removeMonosyllables },
    { label: "특수문자 제거", value: removeNumbers },
    { label: "단음절 제거", value: removeSpecial },
  ];

  return (
    <div className="flex flex-col gap-3">
      <ResultSection
        title="정제 요약"
        // action={<DownloadButton artifact={artifact} onDownload={onDownload} />}
      >
        <div className="grid grid-cols-3 gap-2 mb-4">
          <StatCard
            label="입력 행"
            value={inputRowCount?.toLocaleString()}
            unit="건"
          />
          <StatCard
            label="정제 후 행"
            value={outputRowCount?.toLocaleString()}
            unit="건"
            sub={`-${droppedCount} 제거됨`}
            subColor="text-red-400"
          />
          <StatCard
            label="정제율"
            value={
              inputRowCount > 0
                ? ((outputRowCount / inputRowCount) * 100).toFixed(1)
                : "-"
            }
            unit="%"
          />
        </div>
        <div className="grid grid-cols-2 gap-2 mb-4">
          <StatCard
            label="원본 문자 수"
            value={sourceInputCharCount?.toLocaleString()}
            unit="건"
          />
          <StatCard
            label="정제 후 문자 수"
            value={cleanedInputCharCount?.toLocaleString()}
            unit="건"
            sub={`-${cleanReducedCharCount?.toLocaleString()} 감소`}
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
          <BarRow
            label="정상 처리"
            value={keptCount}
            total={inputRowCount}
            displayValue={`${keptCount?.toLocaleString()}건`}
            color="bg-emerald-400"
          />
        </div>
      </ResultSection>
    </div>
  );
}
