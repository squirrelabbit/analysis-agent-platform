import { useBuildVersion } from "../hooks/build.query";
import { Card, CardContent } from "@/components/ui/card";
import {
  getStatusColor,
  getStatusIcon,
  getStatusLabel,
} from "@/components/common/Status";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Download, Loader2 } from "lucide-react";
import BuildDialog from "./BuildDialog";
import { isBuildRunning } from "./BuildStatusMeta";
import { useDownloadFile } from "@/shared/apis/common.mutation";
import type { BuildJobType } from "@/shared/types/common";
import { buildLabel } from "@/shared/constants/buildLabels";

interface PipelineCardProps {
  versionId: string;
  type: BuildJobType;
}

function ProgressBar({ percent }: { percent: number }) {
  return (
    <div className="mb-4">
      <div className="flex items-center justify-between mb-2">
        <p className="text-xs text-slate-600">진행률</p>
        <p className="text-xs font-semibold text-slate-900">{percent}%</p>
      </div>
      <div className="w-full bg-slate-200 rounded-full h-2">
        <div
          className={`h-2 rounded-full transition-all ${
            percent === 100
              ? "bg-emerald-500"
              : percent > 0
                ? "bg-blue-500"
                : "bg-slate-300"
          }`}
          style={{ width: `${percent}%` }}
        />
      </div>
    </div>
  );
}

export default function PipelineCard({ versionId, type }: PipelineCardProps) {
  const { mutateAsync: onDownload, isPending: isDownloading } =
    useDownloadFile();

  const { data, isLoading } = useBuildVersion(type);
  const status = isLoading ? "running" : (data?.status ?? "not_requested");
  const buildType = data?.buildType ?? type;
  const percent = data?.progress?.percent ?? 0;
  const running = isBuildRunning(status);

  return (
    <Card className="flex-1 ring-0 border-zinc-100 hover:shadow-md transition-shadow shadow-sm">
      <CardContent>
        <div className="flex items-start justify-between mb-4">
          <div className="flex items-center gap-3">
            {getStatusIcon(status)}
            <div>
              <h3 className="text-[15px] font-semibold text-slate-900">
                {buildLabel(buildType as BuildJobType)}
              </h3>
            </div>
          </div>
          <Badge className={`${getStatusColor(status)}`}>
            {getStatusLabel(status)}
          </Badge>
        </div>

        <ProgressBar percent={percent} />
        <div className="flex gap-2">
          {status === "completed" && (
            <Button
              size="sm"
              variant="outline"
              className="flex-1"
              disabled={isDownloading || running}
              onClick={async () =>
                onDownload({ versionId: versionId, type: type })
              }
            >
              {isDownloading ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  다운로드 중…
                </>
              ) : (
                <>
                  <Download className="w-4 h-4 mr-2" />
                  다운로드
                </>
              )}
            </Button>
          )}
          <BuildDialog
            formId={`${type}-form`}
            stage={type}
            status={status}
            disabled={isDownloading}
          />
        </div>
      </CardContent>
    </Card>
  );
}
