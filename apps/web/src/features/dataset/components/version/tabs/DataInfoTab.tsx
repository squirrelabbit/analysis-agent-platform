import type { DatasetVersion } from "@/features/dataset/types/datasetVersion";
import { formatFileSize } from "@/lib/utils";
import {
  Item,
  ItemContent,
  ItemDescription,
  ItemTitle,
} from "@/components/ui/item";
import { PiplineSummary } from "./PiplineSummary";

export default function DataInfoTab(props: DatasetVersion) {
  const { buildStages, sourceSummary, metadata } = props || {};

  const infos = [
    {
      label: "총 레코드",
      value: sourceSummary.rowCount?.toLocaleString(),
      sub: "건",
    },
    {
      label: "컬럼 수",
      value: sourceSummary.columnCount?.toLocaleString(),
      sub: "개",
    },
    {
      label: "파일 크기",
      value: formatFileSize(metadata.upload.byte_size),
      sub: "",
    },
  ];

  return (
    <div>
      <div className="grid grid-cols-3 gap-3 my-3">
        {infos.map((i) => (
          <Item key={i.label} className="bg-white shadow-sm" variant="outline">
            <ItemContent>
              <ItemDescription className="text-xs">{i.label}</ItemDescription>
              <ItemTitle className="font-bold text-xl">{i.value}</ItemTitle>
              <ItemDescription className="text-xs">{i.sub}</ItemDescription>
            </ItemContent>
          </Item>
        ))}
      </div>
      <Item className="bg-white shadow-sm my-3" variant="outline">
        <ItemContent>
          <ItemTitle>컬럼 목록</ItemTitle>
          <div className=" flex gap-2 pt-2">
            {!metadata?.text_columns
              ? "-"
              : metadata.text_columns.map((col: any) => (
                  <span
                    key={col}
                    className="px-2.5 py-1 rounded-md text-xs font-mono font-medium bg-muted border border-border text-foreground"
                  >
                    {col}
                  </span>
                ))}
          </div>
        </ItemContent>
      </Item>
      <Item className="bg-white shadow-sm" variant="outline">
        <ItemContent>
          <ItemTitle>파이프라인</ItemTitle>
          <PiplineSummary buildStages={buildStages} />
        </ItemContent>
      </Item>
    </div>
  );
}
