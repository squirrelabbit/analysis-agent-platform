import {
  Item,
  ItemActions,
  ItemContent,
  ItemHeader,
} from "@/components/ui/item";
import type { DatasetVersion } from "../../types/datasetVersion";
import { cn, formatFileSize } from "@/lib/utils";
import { Badge } from "@/components/ui/badge";
import {
  useActiveVersion,
  useDownloadVersion,
  useRemoveVersion,
} from "../../hooks/useVersionMutation";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";
import { Switch } from "@/components/ui/switch";
import { Calendar, Database, FileText } from "lucide-react";
import FileDownload from "@/components/common/files/FileDownload";

export default function DatasetVersionItem({
  version,
  isSelected,
  onSelect,
}: {
  version: Omit<DatasetVersion, "sourceSummary">;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const { metadata, recordCount, isActive } = version;

  const active = useActiveVersion();
  const remove = useRemoveVersion();
  const download = useDownloadVersion();

  return (
    <Item
      onClick={onSelect}
      className={cn(
        "hover:bg-blue-50",
        isSelected && "bg-blue-50 hover:bg-blue-50",
      )}
    >
      <ItemHeader>
        <div className="flex items-center gap-2">
          <p>{metadata.upload.original_filename}</p>
          <Badge
            variant="outline"
            className={cn(
              "h-4 px-1.5 text-[10px]",
              isActive
                ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                : "bg-muted/50 text-muted-foreground",
            )}
          >
            {isActive ? "활성" : "비활성"}
          </Badge>
        </div>
        <ItemActions onClick={(e) => e.stopPropagation()}>
          <Switch
            checked={isActive}
            onCheckedChange={() => {
              if (!isActive) {
                active.mutateAsync({
                  projectId: version.projectId,
                  datasetId: version.datasetId,
                  versionId: version.id,
                });
              }
            }}
            className={cn(
              "scale-75 origin-right",
              isActive && "data-[state=checked]:bg-emerald-500",
            )}
          />
          <DeleteDialog
            onDelete={() =>
              remove.mutateAsync({
                projectId: version.projectId,
                datasetId: version.datasetId,
                versionId: version.id,
              })
            }
            title="데이터 버전"
          />
        </ItemActions>
      </ItemHeader>
      <ItemContent>
        {/* 메타 정보 */}
        <div className="flex items-center gap-2 text-xs text-muted-foreground">
          <span className="text-[10px] flex items-center gap-1">
            <Calendar className="w-2.5 h-2.5" />
            {metadata.upload.uploaded_at.slice(0, 10)}
          </span>{" "}
          ·
          <span className="text-[10px] flex items-center gap-1">
            <Database className="w-2.5 h-2.5" />
            {recordCount?.toLocaleString()}건
          </span>{" "}
          ·
          <span className="text-[10px] flex items-center gap-1">
            <FileText className="w-2.5 h-2.5" />
            {formatFileSize(metadata.upload.byte_size)}
          </span>
          ·
          <span>
            <FileDownload
              onClick={() =>
                download.mutateAsync({
                  projectId: version.projectId,
                  datasetId: version.datasetId,
                  versionId: version.id,
                  type: "source",
                })
              }
            />
          </span>
        </div>
      </ItemContent>
    </Item>
  );
}
