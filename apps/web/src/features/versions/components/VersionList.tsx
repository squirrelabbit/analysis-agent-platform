
import { Badge } from "@/components/ui/badge";
import { useDownloadFile } from "@/shared/apis/common.mutation";
import { fmtDate, formatFileSize } from "@/shared/utils/format";
import { Calendar, Database, FileText } from "lucide-react";
import FileDownload from "@/components/common/FileDownload";
import { useNavigate } from "react-router-dom";
import { useVersions } from "../hooks/version.query";
import { useActiveVersion, useDeleteVersion } from "../hooks/version.mutation";
import BaseCard from "@/components/common/cards/BaseCard";
import { CardAction, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import { Switch } from "@/components/ui/switch";
import { cn } from "@/shared/utils/common";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";
import type { BuildJobType } from "@/shared/types/common";
import type { Version } from "../models/version";

type Status = {
  id: BuildJobType;
  label: string;
  status: string;
};

function BadgeStatus(version: Version) {
  const { cleanStatus, docGenuinenessStatus, clauseLabelStatus } = version;
  const status: Status[] = [
    { id: "clean", label: "정제", status: cleanStatus },
    {
      id: "doc_genuineness",
      label: "진성 분류",
      status: docGenuinenessStatus,
    },
    {
      id: "clause_label",
      label: "절 라벨링",
      status: clauseLabelStatus,
    },
  ];
  return (
    <div className="flex gap-2">
      {status.map(({ id, label, status }) => (
        <Badge key={id} variant="secondary" className="text-xs text-zinc-500">
          <span
            className={`w-1.5 h-1.5 rounded-full shrink-0 mr-1 ${status !== "ready" ? "bg-zinc-300" : "bg-emerald-500"}`}
          />
          {label}
        </Badge>
      ))}
    </div>
  );
}

function MetaData(version: Version) {
  const { id, createdAt, rowCount, byteSize } = version;
  const { mutateAsync: onDownload } = useDownloadFile();

  return (
    <div className="flex items-center gap-2 text-xs text-muted-foreground">
      <span className="text-xs flex items-center gap-1">
        <Calendar className="w-2.5 h-2.5" />
        {fmtDate(createdAt)}
      </span>{" "}
      ·
      <span className="text-xs flex items-center gap-1">
        <Database className="w-2.5 h-2.5" />
        {rowCount}건
      </span>{" "}
      ·
      <span className="text-xs flex items-center gap-1">
        <FileText className="w-2.5 h-2.5" />
        {formatFileSize(byteSize)}
      </span>
      ·
      <span>
        <FileDownload
          onClick={async () => onDownload({ versionId: id, type: "source" })}
        />
      </span>
    </div>
  );
}

export function VersionList() {
  const navigate = useNavigate();
  const { data: versions = [], isLoading } = useVersions();

  const { mutateAsync: onActive } = useActiveVersion();
  const { mutateAsync: onDelete } = useDeleteVersion();

  if (isLoading) return <></>;

  return (
    <div className="flex flex-col gap-3">
      {versions.map((version) => {
        const { id, originalFilename, isActive } = version;
        return (
          <BaseCard key={version.id} onClick={() => navigate(id)}>
            <CardHeader>
              <CardTitle>{originalFilename}</CardTitle>
                <MetaData {...version} />
              <CardAction onClick={(e) => e.stopPropagation()}>
                <Switch
                  className={cn(
                    isActive && "data-[state=checked]:bg-emerald-500",
                  )}
                  checked={isActive}
                  onCheckedChange={() => {
                    if (!isActive) onActive(id);
                  }}
                />
                <DeleteDialog
                  title="데이터 버전"
                  onDelete={() => onDelete(id)}
                />
              </CardAction>
            </CardHeader>
            <CardFooter>
              <BadgeStatus {...version} />
            </CardFooter>
          </BaseCard>
        );
      })}
    </div>
  );
}
