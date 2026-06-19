import { useState } from "react";
import { FileText, Loader2, Pencil, X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { ReportPanelApi } from "../hooks/useReportPanel";
import ReportPanelCard, { type DropHint } from "./ReportPanelCard";

// 시안 「분석 채팅 - 보고서 패널」 우측 슬라이드 패널.
// 헤더(제목/카운트/닫기) · 보고서 제목바 · 카드 리스트(빈 상태) · 푸터(비우기 + 보고서 만들기).
// 폭 0 ↔ 432px 트랜지션으로 슬라이드.

interface ReportPanelProps {
  panel: ReportPanelApi;
  onCreate: () => void;
  creating: boolean;
}

interface DropTarget {
  index: number;
  after: boolean;
}

export default function ReportPanel({ panel, onCreate, creating }: ReportPanelProps) {
  const {
    staged,
    panelOpen,
    reportTitle,
    count,
    messageOf,
    cardStateOf,
    setReportTitle,
    setTitle,
    setNote,
    remove,
    reorder,
    clearAll,
    closePanel,
  } = panel;

  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [dropTarget, setDropTarget] = useState<DropTarget | null>(null);

  function handleDrop() {
    if (dragIndex !== null && dropTarget !== null && dropTarget.index !== dragIndex) {
      const from = dragIndex;
      let to = dropTarget.index + (dropTarget.after ? 1 : 0);
      if (from < to) to -= 1;
      reorder(from, to);
    }
    setDragIndex(null);
    setDropTarget(null);
  }

  function dropHintFor(index: number): DropHint {
    if (dragIndex === null || dropTarget === null || dropTarget.index !== index)
      return null;
    if (index === dragIndex) return null;
    return dropTarget.after ? "after" : "before";
  }

  return (
    <div
      className={cn(
        "flex shrink-0 flex-col overflow-hidden border-l border-zinc-200 bg-white transition-[width] duration-300 ease-in-out",
        panelOpen ? "w-[432px]" : "w-0",
      )}
    >
      <div className="flex h-full w-[432px] flex-col">
        {/* header */}
        <div className="flex shrink-0 items-center gap-2.5 border-b border-zinc-200 px-4 py-3.5">
          <span className="grid h-7.5 w-7.5 place-items-center rounded-lg bg-violet-50 text-violet-700">
            <FileText className="h-4.25 w-4.25" />
          </span>
          <span className="text-[14.5px] font-bold text-zinc-900">보고서 구성</span>
          <span className="grid h-5.25 min-w-5.25 place-items-center rounded-full bg-violet-600 px-1.75 text-xs font-extrabold text-white">
            {count}
          </span>
          <button
            type="button"
            onClick={closePanel}
            title="패널 닫기"
            className="ml-auto grid h-8 w-8 place-items-center rounded-lg text-zinc-400 transition hover:bg-zinc-100 hover:text-zinc-900"
          >
            <X className="h-4.5 w-4.5" />
          </button>
        </div>

        {count === 0 ? (
          /* empty state */
          <div className="flex flex-1 flex-col items-center justify-center gap-3.5 px-7 text-center">
            <span className="grid h-13 w-13 place-items-center rounded-[15px] bg-zinc-100 text-zinc-400">
              <FileText className="h-6.5 w-6.5" />
            </span>
            <h4 className="text-[14.5px] font-bold text-zinc-600">
              아직 추가된 결과가 없습니다
            </h4>
            <p className="max-w-[230px] text-[12.5px] leading-relaxed text-zinc-400">
              분석 결과 카드의 <b className="font-semibold">보고서에 추가</b>를 누르면
              여기에 쌓이고, 한 번에 보고서로 만들 수 있습니다.
            </p>
          </div>
        ) : (
          <>
            {/* report title */}
            <div className="shrink-0 border-b border-zinc-200 bg-zinc-50/70 px-4 py-3">
              <label className="mb-1.5 block text-[10.5px] font-extrabold uppercase tracking-wide text-zinc-400">
                보고서 제목
              </label>
              <div className="flex items-center gap-2 rounded-lg border border-transparent px-1.5 py-1 transition hover:bg-zinc-100 focus-within:border-violet-500 focus-within:bg-white focus-within:ring-2 focus-within:ring-violet-100">
                <Pencil className="h-3.75 w-3.75 shrink-0 text-zinc-400" />
                <input
                  value={reportTitle}
                  onChange={(e) => setReportTitle(e.target.value)}
                  placeholder="제목 없는 보고서"
                  className="min-w-0 flex-1 bg-transparent text-[15px] font-bold text-zinc-900 outline-none placeholder:font-bold placeholder:text-zinc-400"
                />
              </div>
            </div>

            {/* card list */}
            <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-3.5">
              {staged.map((runId, index) => {
                const msg = messageOf(runId);
                if (!msg) return null;
                const cs = cardStateOf(runId);
                return (
                  <ReportPanelCard
                    key={runId}
                    message={msg}
                    index={index}
                    title={cs.title}
                    note={cs.note}
                    onTitleChange={(v) => setTitle(runId, v)}
                    onNoteChange={(v) => setNote(runId, v)}
                    onRemove={() => remove(runId)}
                    isDragging={dragIndex === index}
                    dropHint={dropHintFor(index)}
                    onDragStartCard={() => setDragIndex(index)}
                    onDragOverCard={(after) => setDropTarget({ index, after })}
                    onDropCard={handleDrop}
                    onDragEndCard={() => {
                      setDragIndex(null);
                      setDropTarget(null);
                    }}
                  />
                );
              })}
            </div>

            {/* footer */}
            <div className="flex shrink-0 gap-2.5 border-t border-zinc-200 px-3.5 py-3">
              <button
                type="button"
                onClick={clearAll}
                disabled={creating}
                title="전체 비우기"
                className="grid h-10 w-11 place-items-center rounded-xl border border-zinc-200 bg-white text-zinc-500 transition hover:border-zinc-300 hover:text-zinc-900 disabled:opacity-50"
              >
                <X className="h-4 w-4" />
              </button>
              <button
                type="button"
                onClick={onCreate}
                disabled={creating}
                className="flex h-10 flex-1 items-center justify-center gap-1.75 rounded-xl bg-violet-600 text-[13.5px] font-bold text-white transition hover:bg-violet-700 disabled:opacity-60"
              >
                {creating ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    만드는 중…
                  </>
                ) : (
                  <>
                    <FileText className="h-4 w-4" />
                    보고서 만들기
                  </>
                )}
              </button>
            </div>
          </>
        )}
      </div>
    </div>
  );
}
