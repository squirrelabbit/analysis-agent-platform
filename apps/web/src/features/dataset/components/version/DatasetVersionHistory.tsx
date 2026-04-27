import {
  Item,
  ItemContent,
  ItemDescription,
  ItemHeader,
  ItemTitle,
} from "@/components/ui/item";
import type { DatasetVersion } from "../../types/datasetVersion";
import DatasetversionItem from "./DatasetVersionItem";
import { useState } from "react";
import { useDatasetVersionDetail } from "../../hooks/useDatasetVersion";
import DatasetVersionDetail from "./DatasetVersionDetail";
import { Separator } from "@/components/ui/separator";
import { EmptyForm } from "@/components/common/EmptyForm";
import { Upload } from "lucide-react";
import { VersionDetailSkeleton } from "./VersionSkeleton";

export default function DatasetVersionHistory({
  versions,
  projectId,
  datasetId,
}: {
  versions: DatasetVersion[];
  projectId: string;
  datasetId: string;
}) {
  const [selected, setSelected] = useState(
    versions.find((v) => v.isActive)?.id ?? versions[0]?.id,
  );
  const { data: detail, isLoading } = useDatasetVersionDetail(
    projectId,
    datasetId,
    selected,
  );

  console.log(versions)

  if (versions.length === 0) {
    return <EmptyForm
      title="등록된 데이터가 없습니다"
      description="데이터 파일을 업로드하세요"
      icon={<Upload className="text-zinc-400" />}
    />
  }

  return (
    <div className="flex flex-1 rounded-xl border border-border shadow-sm overflow-x-auto bg-background">
      <div className="w-75 shrink-0 border-r border-border">
        <Item className="p-0">
          <ItemHeader className="m-4 mb-2">
            <ItemTitle className="font-bold">버전 이력</ItemTitle>
            <ItemDescription className="text-xs">
              전체 {versions.length}개
            </ItemDescription>
          </ItemHeader>
          <Separator />
          <ItemContent className="gap-0">
            {versions.map((v) => (
              <DatasetversionItem
                key={v.id}
                version={v}
                isSelected={selected === v.id}
                onSelect={() => setSelected(v.id)}
              />
            ))}
          </ItemContent>
        </Item>
      </div>
      <div className="flex-1 ">
        {isLoading && <VersionDetailSkeleton />}
        {!isLoading && detail && <DatasetVersionDetail version={detail} />}
        {!isLoading && !detail && <div>버전을 선택하세요</div>}
      </div>
    </div>
  );
}
