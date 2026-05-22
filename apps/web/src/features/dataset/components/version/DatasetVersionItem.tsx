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
import { useParams } from "react-router-dom";
import { fmtDate } from "@/utils/format";

export default function DatasetVersionItem({
  version,
  isSelected,
  onSelect,
}: {
  version: Omit<DatasetVersion, "sourceSummary">;
  isSelected: boolean;
  onSelect: () => void;
}) {
  const { projectId, datasetId } = useParams();
  const {
    // id,
    originalFilename,
    createdAt,
    isActive,
    rowCount,
    // columnCount,
    byteSize,
  } = version;

  const active = useActiveVersion();
  const remove = useRemoveVersion();
  const download = useDownloadVersion();

  if (!projectId || !datasetId) return null;
  return (
    <Item
      role="button"
      className={cn(
        "group cursor-pointer",

        // animation
        "transition-all duration-200 ease-out",

        // hover
        "hover:bg-violet-50 hover:border-violet-200",
        "hover:-translate-y-px",
        "hover:shadow-sm",

        // focus
        "focus-visible:outline-none",
        "focus-visible:ring-2 focus-visible:ring-violet-400/40",

        // selected
        isSelected && ["bg-violet-50", "border-violet-200", "shadow-sm"],
      )}
      // className={cn(
      //   "hover:bg-blue-50 hover:cursor-pointer",
      //   isSelected && "bg-blue-50 hover:bg-blue-50",
      // )}
      tabIndex={0}
      onClick={onSelect}
      onKeyDown={(e) => {
        if (e.key === "Enter" || e.key === " ") {
          onSelect();
        }
      }}
    >
      <ItemHeader>
        <div className="flex items-center gap-2">
          <p>{originalFilename}</p>
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
                  projectId: projectId,
                  datasetId: datasetId,
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
                projectId: projectId,
                datasetId: datasetId,
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
            {fmtDate(createdAt)}
          </span>{" "}
          ·
          <span className="text-[10px] flex items-center gap-1">
            <Database className="w-2.5 h-2.5" />
            {rowCount}건
          </span>{" "}
          ·
          <span className="text-[10px] flex items-center gap-1">
            <FileText className="w-2.5 h-2.5" />
            {formatFileSize(byteSize)}
          </span>
          ·
          <span>
            <FileDownload
              onClick={() =>
                download.mutateAsync({
                  projectId: projectId,
                  datasetId: datasetId,
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
