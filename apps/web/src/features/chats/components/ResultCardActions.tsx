import { useRef, useState } from "react";
import {
  Check,
  Copy,
  Download,
  FilePlus2,
  Loader2,
  RefreshCw,
  Share2,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";
import { buildCopyText, buildCsv, downloadCsv, hasTableData } from "../utils/resultExport";

// 디자인 「결과 카드 액션 아이콘」 Option C — 전부 고스트 아이콘(최소).
// 왼쪽 "결과 액션" 힌트 + 오른쪽 고스트 아이콘 그룹. 1차 액션(보고서 저장)만
// violet, vrule로 보조군과 구분. 저장/복사/다운로드는 동작, 다시생성·공유는
// 토스트 스텁(백엔드 없음 — 정직하게 "준비 중" 안내). (silverone 2026-06-11)

export type ReportSaveState = "idle" | "saving" | "saved";

// 34px 히트영역 + 18px 글리프 + 고스트(평소 회색, hover 잉크/연회색 칩).
const ICONBTN =
  "inline-grid place-items-center h-[34px] w-[34px] rounded-[9px] border border-transparent " +
  "transition-colors hover:bg-zinc-100 hover:text-zinc-900 active:bg-zinc-200 " +
  "disabled:pointer-events-none disabled:opacity-50";
const GLYPH = "h-[18px] w-[18px]";

interface ResultCardActionsProps {
  message: ChatMessage;
  onSave?: () => void;
  saveState?: ReportSaveState;
  onToast: (msg: string) => void;
}

export default function ResultCardActions({
  message,
  onSave,
  saveState = "idle",
  onToast,
}: ResultCardActionsProps) {
  const [copyDone, setCopyDone] = useState(false);
  const copyTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const regenRef = useRef<SVGSVGElement>(null);

  const canExport = hasTableData(message);

  async function handleCopy() {
    const text = buildCopyText(message);
    try {
      await navigator.clipboard.writeText(text);
      setCopyDone(true);
      if (copyTimer.current) clearTimeout(copyTimer.current);
      copyTimer.current = setTimeout(() => setCopyDone(false), 1100);
      onToast("결과를 클립보드에 복사했습니다");
    } catch {
      onToast("복사에 실패했습니다");
    }
  }

  function handleDownload() {
    const csv = buildCsv(message);
    if (!csv) {
      onToast("내보낼 표 데이터가 없는 결과입니다");
      return;
    }
    downloadCsv(csv);
    onToast("CSV로 내보냈습니다");
  }

  function handleRegen() {
    // 다시 생성은 아직 미연동 — 클릭 피드백으로 짧게 회전만, 동작은 안내.
    regenRef.current?.animate(
      [{ transform: "rotate(0)" }, { transform: "rotate(360deg)" }],
      { duration: 700, easing: "ease-in-out" },
    );
    onToast("다시 생성은 곧 지원됩니다");
  }

  return (
    <div className="mt-2 flex items-center border-t border-zinc-100 px-1 pt-2">
      <span className="pl-1 text-[11.5px] font-semibold text-zinc-400">
        결과 액션
      </span>
      <div className="flex-1" />
      <div className="flex items-center gap-0.5">
        {/* 1차 액션 — 보고서에 저장 (violet, 아이콘만) */}
        {onSave && (
          <button
            type="button"
            onClick={onSave}
            disabled={saveState !== "idle"}
            title={
              saveState === "saved"
                ? "보고서에 저장됨"
                : "이 결과를 보고서에 저장"
            }
            className={cn(
              ICONBTN,
              saveState === "saved"
                ? "text-emerald-600"
                : "text-violet-700 hover:bg-violet-50 hover:text-violet-800",
              saveState === "saving" && "cursor-wait",
            )}
          >
            {saveState === "saving" ? (
              <Loader2 className={cn(GLYPH, "animate-spin")} />
            ) : saveState === "saved" ? (
              <Check className={GLYPH} strokeWidth={2.4} />
            ) : (
              <FilePlus2 className={GLYPH} />
            )}
          </button>
        )}

        {onSave && <span className="mx-[3px] h-[18px] w-px bg-zinc-200" />}

        {/* 복사 */}
        <button
          type="button"
          onClick={handleCopy}
          title="복사"
          className={cn(ICONBTN, copyDone ? "text-emerald-600" : "text-zinc-400")}
        >
          {copyDone ? (
            <Check className={GLYPH} strokeWidth={2.4} />
          ) : (
            <Copy className={GLYPH} />
          )}
        </button>

        {/* 다운로드 (CSV) */}
        <button
          type="button"
          onClick={handleDownload}
          disabled={!canExport}
          title={canExport ? "CSV로 내보내기" : "내보낼 표 데이터 없음"}
          className={cn(ICONBTN, "text-zinc-400")}
        >
          <Download className={GLYPH} />
        </button>

        {/* 다시 생성 (스텁) */}
        <button
          type="button"
          onClick={handleRegen}
          title="다시 생성"
          className={cn(ICONBTN, "text-zinc-400")}
        >
          <RefreshCw ref={regenRef} className={GLYPH} />
        </button>

        {/* 공유 (스텁) */}
        <button
          type="button"
          onClick={() => onToast("공유는 곧 지원됩니다")}
          title="공유"
          className={cn(ICONBTN, "text-zinc-400")}
        >
          <Share2 className={GLYPH} />
        </button>
      </div>
    </div>
  );
}
