import { useRef, useState } from "react";
import {
  Check,
  Copy,
  Download,
  FilePlus2,
  RefreshCw,
  Share2,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";
import { buildCopyText, buildCsv, downloadCsv, hasTableData } from "../utils/resultExport";

// 시안 「분석 채팅 - 보고서 패널」 결과 액션바.
// 1차 액션 = "보고서에 추가"(패널에 적재) — violet 라벨 버튼. 추가되면 "추가됨"(emerald, 비활성).
// 보조군(복사/CSV/다시생성/공유)은 고스트 아이콘. 복사/CSV는 동작, 다시생성·공유는
// 토스트 스텁(백엔드 없음 — 정직하게 "준비 중" 안내). (silverone 2026-06-11)

// 34px 히트영역 + 18px 글리프 + 고스트(평소 회색, hover 잉크/연회색 칩).
const ICONBTN =
  "inline-grid place-items-center h-[34px] w-[34px] rounded-[9px] border border-transparent " +
  "transition-colors hover:bg-zinc-100 hover:text-zinc-900 active:bg-zinc-200 " +
  "disabled:pointer-events-none disabled:opacity-50";
const GLYPH = "h-[18px] w-[18px]";

interface ResultCardActionsProps {
  message: ChatMessage;
  // 보고서 패널 적재 — assistant 결과 메시지(runId 보유)에만 연결. 없으면 1차 액션 미노출.
  onAddToReport?: () => void;
  isAdded?: boolean;
  onToast: (msg: string) => void;
}

export default function ResultCardActions({
  message,
  onAddToReport,
  isAdded = false,
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
      {/* 1차 액션 — 보고서에 추가(패널 적재) */}
      {onAddToReport && (
        <button
          type="button"
          onClick={onAddToReport}
          disabled={isAdded}
          title={isAdded ? "보고서에 추가됨" : "이 결과를 보고서에 추가"}
          className={cn(
            "inline-flex h-[34px] items-center gap-1.5 rounded-[9px] px-2.5 text-[12.5px] font-semibold transition-colors",
            isAdded
              ? "cursor-default text-emerald-600"
              : "text-violet-700 hover:bg-violet-50 hover:text-violet-800",
          )}
        >
          {isAdded ? (
            <>
              <Check className="h-[17px] w-[17px]" strokeWidth={2.4} />
              추가됨
            </>
          ) : (
            <>
              <FilePlus2 className="h-[17px] w-[17px]" />
              보고서에 추가
            </>
          )}
        </button>
      )}
      <div className="flex-1" />
      <div className="flex items-center gap-0.5">
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
