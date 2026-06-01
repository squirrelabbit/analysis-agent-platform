import { AlertOctagon, Loader2 } from "lucide-react";
import type { RunStatus as RunStatusValue } from "../models";

const DEFAULT_ERROR_MESSAGE = "분석 실행 중 오류가 발생했습니다.";

interface Props {
  status?: RunStatusValue;
  error?: string;
}

// run.status 표시 정책:
// - completed: 성공은 굳이 강조하지 않음 → 아무것도 렌더하지 않음
// - running: 작은 회색 배지 ("실행 중")
// - failed: 빨간 에러 박스 (error_message 또는 기본 문구)
export default function RunStatus({ status, error }: Props) {
  if (!status) return null;

  if (status === "completed") return null;

  if (status === "running") {
    return (
      <div className="mt-2 inline-flex items-center gap-1.5 rounded-md bg-zinc-50 border border-zinc-100 px-2 py-1 text-[11px] text-zinc-500">
        <Loader2 className="w-3 h-3 animate-spin" />
        <span>실행 중</span>
      </div>
    );
  }

  // failed
  return (
    <div className="mt-2 flex items-start gap-1.5 rounded-md border border-red-200 bg-red-50 px-2.5 py-1.5 text-[11px] text-red-700">
      <AlertOctagon className="w-3 h-3 mt-0.5 shrink-0" />
      <span className="break-words">{error || DEFAULT_ERROR_MESSAGE}</span>
    </div>
  );
}
