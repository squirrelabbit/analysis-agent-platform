import { Clock, Loader2 } from "lucide-react";
import { formatSecond } from "@/shared/utils/format";
import type { ProgressType } from "../models/build";

// silverone 2026-06-08 — 빌드 탭에서 "완료된 결과" vs "새로 진행 중인 빌드"를
// 시각적으로 구분한다. 백엔드는 status(running/queued/completed/failed) + progress +
// live durationSeconds를 이미 내려주지만, 탭들이 이를 안 써서 진행 중에도 완료처럼
// 보였다(소요시간만 live로 증가). 시간 칩 라벨 + 진행 배너로 구분한다.

type BuildStatus = string | undefined;

export const isBuildRunning = (status: BuildStatus): boolean =>
  status === "running" || status === "queued";

// 메타 행의 시간 칩. 진행 중이면 "경과 시간" + 진행 배지, 완료면 "소요 시간"(고정).
export function BuildTimerChip({
  status,
  durationSeconds,
}: {
  status: BuildStatus;
  durationSeconds?: number;
}) {
  const running = isBuildRunning(status);
  return (
    <span className="inline-flex items-center gap-1.5 font-medium">
      <Clock className="h-3.5 w-3.5 text-zinc-400" strokeWidth={1.8} />
      {running ? "경과 시간" : "소요 시간"}
      <b className="font-bold text-zinc-800">{formatSecond(durationSeconds)}</b>
      {running && (
        <span className="inline-flex items-center gap-1 rounded-full bg-blue-50 px-2 py-0.5 text-[11px] font-semibold text-blue-600">
          <Loader2 className="h-3 w-3 animate-spin" strokeWidth={2} />
          {status === "queued" ? "대기 중" : "진행 중"}
        </span>
      )}
    </span>
  );
}

// 진행 중일 때 메타 행 아래에 표시하는 배너 + 진행률 바. 완료/실패 상태면 렌더 안 함.
// hasPrevious=true면 화면의 결과가 '이전 빌드' 결과임을 안내한다.
export function BuildRunningBanner({
  status,
  progress,
  hasPrevious,
}: {
  status: BuildStatus;
  progress?: ProgressType;
  hasPrevious: boolean;
}) {
  if (!isBuildRunning(status)) return null;
  const pct = Math.min(100, Math.max(0, progress?.percent ?? 0));
  const processed = progress?.processedRows;
  const total = progress?.totalRows;
  const eta = progress?.etaSeconds;
  return (
    <div className="rounded-2xl border border-blue-200 bg-blue-50/70 p-4">
      <div className="flex items-center gap-2 text-[13px] font-bold text-blue-700">
        <Loader2 className="h-4 w-4 animate-spin" strokeWidth={2} />
        {status === "queued" ? "새 빌드 대기 중…" : "새 빌드 진행 중…"}
      </div>
      {total ? (
        <div className="mt-1 text-xs font-medium text-blue-600/90">
          {processed?.toLocaleString()} / {total?.toLocaleString()} ·{" "}
          {pct.toFixed(0)}%
          {eta != null && eta > 0 ? ` · 남은 시간 ~${formatSecond(eta)}` : ""}
        </div>
      ) : null}
      <div className="mt-2 h-2 overflow-hidden rounded-full bg-blue-100">
        <div
          className="h-full rounded-full bg-blue-500 transition-all duration-700"
          style={{ width: `${pct}%` }}
        />
      </div>
      {hasPrevious && (
        <div className="mt-2 text-[11px] font-medium text-blue-600/80">
          아래는 이전 빌드 결과입니다. 완료되면 자동으로 갱신됩니다.
        </div>
      )}
    </div>
  );
}
