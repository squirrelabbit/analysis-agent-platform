import type { DatasetVersionDetail } from "@/features/dataset/types/datasetVersion";
import { formatFileSize } from "@/lib/utils";
import {
  Item,
  ItemContent,
  ItemDescription,
  ItemTitle,
} from "@/components/ui/item";

export default function DataInfoTab(props: DatasetVersionDetail) {
  const { rowCount, columnCount, columns, byteSize } = props || {};

  const infos = [
    {
      label: "총 레코드",
      value: rowCount.toLocaleString(),
      sub: "건",
    },
    {
      label: "컬럼 수",
      value: columnCount.toLocaleString(),
      sub: "개",
    },
    {
      label: "파일 크기",
      value: formatFileSize(byteSize),
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
          <div className="flex flex-wrap gap-2 pt-2">
            {!columns.length
              ? "-"
              : columns.map((col: any) => (
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
      {/* <Item className="bg-white shadow-sm" variant="outline">
        <ItemContent>
          <ItemTitle>파이프라인 / 처리 상태</ItemTitle>
        </ItemContent>
      </Item> */}
    </div>
  );
}
