import { useEffect, useState } from "react";
import { useBuildVersion } from "../hooks/build.query";
import { useCancelBuildJob } from "../hooks/build.mutation";
import { Card, CardContent } from "@/components/ui/card";
import {
  getStatusColor,
  getStatusIcon,
  getStatusLabel,
} from "@/components/common/Status";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Download, Loader2, Square } from "lucide-react";
import BuildDialog from "./BuildDialog";
import { isBuildRunning } from "./BuildStatusMeta";
import { useDownloadFile } from "@/shared/apis/common.mutation";
import ChatToast from "@/features/chats/components/ChatToast";
import type { BuildJobType } from "@/shared/types/common";
import { buildLabel } from "@/shared/constants/buildLabels";

// 중단(협조적 취소) 지원 단계 — worker가 처리 루프에서 신호를 확인한다. clean은 제외.
const CANCELLABLE_BUILD_TYPES: BuildJobType[] = [
  "doc_genuineness",
  "clause_label",
  "clause_keywords",
];

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

/** 빌드 상태 조회 전(첫 로딩) 표시하는 카드 스켈레톤 */
function PipelineCardSkeleton() {
  return (
    <Card className="ring-0 flex-1 border-zinc-100 shadow-sm">
      <CardContent>
        <div className="mb-4 flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="h-5 w-5 animate-pulse rounded-full bg-zinc-100" />
            <div className="h-4 w-24 animate-pulse rounded bg-zinc-100" />
          </div>
          <div className="h-5 w-14 animate-pulse rounded-full bg-zinc-100" />
        </div>
        <div className="mb-4">
          <div className="mb-2 flex items-center justify-between">
            <div className="h-3 w-10 animate-pulse rounded bg-zinc-100" />
            <div className="h-3 w-8 animate-pulse rounded bg-zinc-100" />
          </div>
          <div className="h-2 w-full animate-pulse rounded-full bg-zinc-100" />
        </div>
        <div className="h-7 w-full animate-pulse rounded-lg bg-zinc-100" />
      </CardContent>
    </Card>
  );
}

export default function PipelineCard({ versionId, type }: PipelineCardProps) {
  const { mutateAsync: onDownload, isPending: isDownloading } =
    useDownloadFile();

  const { mutateAsync: onCancel } = useCancelBuildJob();
  const [toast, setToast] = useState("");
  // 중단 요청 후 빌드가 실제로 멈출 때까지(진행 중 호출 마무리, 수초~수십초) 버튼을
  // 잠가 연타를 막는다. cancel HTTP 자체는 빨리 끝나므로 mutation pending만으론 부족.
  const [cancelRequested, setCancelRequested] = useState(false);

  const { data, isLoading } = useBuildVersion(type);
  const status = data?.status ?? "not_requested";
  const buildType = data?.buildType ?? type;
  const percent = data?.progress?.percent ?? 0;
  const running = isBuildRunning(status);
  const cancellable = running && CANCELLABLE_BUILD_TYPES.includes(type);

  // 빌드가 더 이상 running이 아니면(멈춤/완료) 중단 요청 상태 해제.
  useEffect(() => {
    if (!running) setCancelRequested(false);
  }, [running]);

  if (isLoading) return <PipelineCardSkeleton />;

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
          {cancellable && (
            <Button
              size="sm"
              variant="outline"
              className="flex-1 text-red-600 hover:bg-red-50 hover:text-red-700 border-red-200"
              disabled={cancelRequested}
              onClick={async () => {
                setCancelRequested(true);
                setToast("중단 요청됨 — 진행 중 작업을 마무리한 뒤 멈춥니다");
                setTimeout(() => setToast(""), 2500);
                try {
                  await onCancel({ type });
                } catch {
                  // 실패 시 다시 누를 수 있게 해제.
                  setCancelRequested(false);
                }
              }}
            >
              {cancelRequested ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  중단 중…
                </>
              ) : (
                <>
                  <Square className="w-4 h-4 mr-2" />
                  중단
                </>
              )}
            </Button>
          )}
          {(status === "completed" && buildType !== 'clause_keywords') && (
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
      <ChatToast message={toast} visible={!!toast} />
    </Card>
  );
}
