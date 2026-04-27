import { Item, ItemContent, ItemHeader } from "@/components/ui/item";
import type { DatasetVersion } from "../../types/datasetVersion";
import { Switch } from "@/components/ui/switch";
import { cn, formatFileSize } from "@/lib/utils";
import { Calendar, Database, FileText } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { useActiveVersion } from "../../hooks/useVersionMutation";

export default function DatasetVersionItem({
  version,
  isSelected,
  onSelect,
}: {
  version: DatasetVersion;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const active = useActiveVersion();
  return (
    <div
      onClick={onSelect}
      className={cn(
        "relative border-b border-border cursor-pointer transition-colors",
        // 좌측 선택 인디케이터
        "before:absolute before:left-0 before:top-0 before:bottom-0 before:w-1 before:rounded-r-none before:transition-colors",
        "hover:bg-muted/50",
        isSelected && "bg-blue-50 before:bg-blue-600 hover:bg-blue-50",
        version.isActive && !isSelected && "before:bg-emerald-500",
      )}
    >
      <Item>
        <ItemHeader>
          <div className="flex items-center gap-4 mb-1.5">
            <p
              className={cn(
                "font-mono truncate",
                isSelected
                  ? "text-primary font-bold"
                  : "text-muted-foreground ",
              )}
            >
              {version.metadata.upload.original_filename}
            </p>
            <Badge
              variant="outline"
              className={cn(
                "h-4 px-1.5 text-[10px]",
                version.isActive
                  ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                  : "bg-muted/50 text-muted-foreground",
              )}
            >
              {version.isActive ? "활성" : "비활성"}
            </Badge>
          </div>
          <Switch
            checked={version.isActive}
            onCheckedChange={() => {
              if (!version.isActive) {
                active.mutateAsync({
                  projectId: version.projectId,
                  datasetId: version.datasetId,
                  versionId: version.id,
                });
              }
            }}
            className={cn(
              "scale-75 origin-right",
              version.isActive && "data-[state=checked]:bg-emerald-500",
            )}
          />
        </ItemHeader>
        <ItemContent>
          {/* 메타 정보 */}
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span className="flex items-center gap-1">
              <Calendar className="w-2.5 h-2.5" />
              {version.metadata.upload.uploaded_at.slice(0, 10)}
            </span>{" "}
            ·
            <span className="flex items-center gap-1">
              <Database className="w-2.5 h-2.5" />
              {version.recordCount.toLocaleString()}건
            </span>{" "}
            ·
            <span className="flex items-center gap-1">
              <FileText className="w-2.5 h-2.5" />
              {formatFileSize(version.metadata.upload.byte_size)}
            </span>
          </div>
        </ItemContent>
      </Item>
    </div>
  );
}
