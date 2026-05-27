import { useBuildVersion } from "../hooks/build.query";
import { Card, CardContent } from "@/components/ui/card";
import {
  getStatusColor,
  getStatusIcon,
  getStatusLabel,
} from "@/components/common/Status";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Download } from "lucide-react";
import BuildDialog from "./BuildDialog";
import { useDownloadFile } from "@/shared/apis/common.mutation";
import type { BuildJobType } from "@/shared/types/common";
import type { ProgressType } from "../models/build";

interface PipelineCardProps {
  versionId: string;
  type: BuildJobType;
}

function ProgressBar({ percent }: ProgressType) {
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
  const { mutateAsync: onDownload } = useDownloadFile();

  const { data } = useBuildVersion(type);
  if (!data) return null;
  const { status, buildType, progress } = data || {};
  return (
    <Card className="flex-1 border-slate-200 hover:shadow-md transition-shadow">
      <CardContent>
        <div className="flex items-start justify-between mb-4">
          <div className="flex items-center gap-3">
            {getStatusIcon(status)}
            <div>
              <h3 className="font-semibold text-slate-900">{buildType}</h3>
            </div>
          </div>
          <Badge className={`${getStatusColor(status)}`}>
            {getStatusLabel(status)}
          </Badge>
        </div>

        <ProgressBar {...progress} />
        {/* Action Buttons */}
        <div className="flex gap-2">
          {status === "completed" && (
            <Button
              size="sm"
              variant="outline"
              className="flex-1"
              onClick={async () =>
                onDownload({ versionId: versionId, type: type })
              }
            >
              <Download className="w-4 h-4 mr-2" />
              다운로드
            </Button>
          )}
          <BuildDialog formId={`${type}-form`} stage={type} status={status} />
        </div>
      </CardContent>
    </Card>
  );
}
