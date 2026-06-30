import { useEffect, useRef, useState } from "react";
import {
  Check,
  ChevronDown,
  FileText,
  Loader2,
  Pencil,
  Plus,
  X,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { ReportSummary } from "@/features/reports/models";
import type { ReportPanelApi } from "../hooks/useReportPanel";
import ReportPanelCard, { type DropHint } from "./ReportPanelCard";

// 채팅 우측 보고서 패널.
//   상단 드롭다운: 기존 보고서 불러오기(이어서 편집 → 저장 시 갱신) / 새 보고서(생성).
//   본문 staging: 기초분석 가져오기 + 결과/섹션 카드(accordion + 드래그 정렬) + 제목/메모.
//   저장: 새 보고서 → POST /reports, 불러온 보고서 → PUT /reports/{id}.

interface ReportPanelProps {
  panel: ReportPanelApi;
  reports: ReportSummary[];
  reportsLoading: boolean;
  onSelectExisting: (reportId: string) => void;
  onNewReport: () => void;
  onSave: () => void;
  saving: boolean;
  templateLoading: boolean;
}

interface DropTarget {
  index: number;
  after: boolean;
}

export default function ReportPanel({
  panel,
  reports,
  reportsLoading,
  onSelectExisting,
  onNewReport,
  onSave,
  saving,
  templateLoading,
}: ReportPanelProps) {
  const {
    staged,
    panelOpen,
    reportTitle,
    count,
    expandedIds,
    loadedReportId,
    setReportTitle,
    setTitle,
    setNote,
    remove,
    reorder,
    toggleExpand,
    clearAll,
    closePanel,
  } = panel;

  // 보고서 선택 드롭다운 — 로컬 UI 상태(바깥 클릭 시 닫힘).
  const [menuOpen, setMenuOpen] = useState(false);
  const selectorRef = useRef<HTMLDivElement>(null);
  const showMenu = menuOpen && panelOpen;

  useEffect(() => {
    if (!showMenu) return;
    function onDocClick(e: MouseEvent) {
      if (!selectorRef.current?.contains(e.target as Node)) setMenuOpen(false);
    }
    document.addEventListener("mousedown", onDocClick);
    return () => document.removeEventListener("mousedown", onDocClick);
  }, [showMenu]);

  const selectionTitle = loadedReportId
    ? reportTitle.trim() ||
      reports.find((r) => r.reportId === loadedReportId)?.title ||
      "제목 없는 보고서"
    : "새 보고서";

  function handleSelect(reportId: string) {
    setMenuOpen(false);
    onSelectExisting(reportId);
  }

  function handleNew() {
    setMenuOpen(false);
    onNewReport();
  }

  // ── 드래그 정렬 + 자동 스크롤 ──
  const [dragIndex, setDragIndex] = useState<number | null>(null);
  const [dropTarget, setDropTarget] = useState<DropTarget | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);
  const autoScroll = useRef<{ vy: number; raf: number }>({ vy: 0, raf: 0 });

  const tickAutoScroll = () => {
    const el = scrollRef.current;
    const { vy } = autoScroll.current;
    if (el && vy !== 0) {
      el.scrollTop += vy;
      autoScroll.current.raf = requestAnimationFrame(tickAutoScroll);
    } else {
      autoScroll.current.raf = 0;
    }
  };

  const updateAutoScroll = (clientY: number) => {
    const el = scrollRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const EDGE = 56;
    const MAX = 16;
    let vy = 0;
    if (clientY < r.top + EDGE)
      vy = -MAX * Math.min(1, (r.top + EDGE - clientY) / EDGE);
    else if (clientY > r.bottom - EDGE)
      vy = MAX * Math.min(1, (clientY - (r.bottom - EDGE)) / EDGE);
    autoScroll.current.vy = vy;
    if (vy !== 0 && !autoScroll.current.raf)
      autoScroll.current.raf = requestAnimationFrame(tickAutoScroll);
  };

  const stopAutoScroll = () => {
    autoScroll.current.vy = 0;
    if (autoScroll.current.raf) {
      cancelAnimationFrame(autoScroll.current.raf);
      autoScroll.current.raf = 0;
    }
  };

  useEffect(() => stopAutoScroll, []);

  function handleDrop() {
    if (
      dragIndex !== null &&
      dropTarget !== null &&
      dropTarget.index !== dragIndex
    ) {
      const from = dragIndex;
      let to = dropTarget.index + (dropTarget.after ? 1 : 0);
      if (from < to) to -= 1;
      reorder(from, to);
    }
    setDragIndex(null);
    setDropTarget(null);
    stopAutoScroll();
  }

  function dropHintFor(index: number): DropHint {
    if (dragIndex === null || dropTarget === null || dropTarget.index !== index)
      return null;
    if (index === dragIndex) return null;
    return dropTarget.after ? "after" : "before";
  }

  return (
    <div className="flex h-full w-full flex-col overflow-hidden bg-white">
      <div className="flex h-full flex-col">
        {/* 상단 보고서 선택 바 */}
        <div className="flex shrink-0 items-center gap-2 border-b border-zinc-200 bg-zinc-50/70 px-3 py-3">
          <div ref={selectorRef} className="relative min-w-0 flex-1">
            <button
              type="button"
              onClick={() => setMenuOpen((o) => !o)}
              disabled={reportsLoading}
              className={cn(
                "flex h-10 w-full items-center gap-2.5 rounded-[10px] border bg-white px-2.5 text-left transition disabled:opacity-60",
                showMenu
                  ? "border-violet-500 ring-[3px] ring-violet-100"
                  : "border-zinc-200 hover:border-zinc-300",
              )}
            >
              <span
                className={cn(
                  "grid h-6.5 w-6.5 shrink-0 place-items-center rounded-lg",
                  loadedReportId
                    ? "bg-violet-50 text-violet-700"
                    : "bg-zinc-100 text-zinc-400",
                )}
              >
                <FileText className="h-3.75 w-3.75" />
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[13.5px] font-bold leading-tight text-zinc-900">
                  {reportsLoading ? "불러오는 중…" : selectionTitle}
                </span>
                <span className="block truncate text-[10.5px] font-semibold text-zinc-400">
                  {loadedReportId ? "기존 보고서 편집 중" : "새 보고서 (저장 안 됨)"} ·
                  블록 {count}개
                </span>
              </span>
              <ChevronDown
                className={cn(
                  "h-4 w-4 shrink-0 text-zinc-400 transition-transform",
                  showMenu && "rotate-180",
                )}
              />
            </button>

            {showMenu && (
              <div className="absolute left-0 right-0 top-[calc(100%+6px)] z-40 max-h-90 overflow-y-auto rounded-xl border border-zinc-200 bg-white p-1.5 shadow-xl">
                <button
                  type="button"
                  onClick={handleNew}
                  className="flex w-full items-center gap-2.5 rounded-[9px] p-2.25 text-left text-[13px] font-bold text-violet-700 transition hover:bg-violet-50"
                >
                  <span className="grid h-7 w-7 shrink-0 place-items-center rounded-lg bg-violet-600 text-white">
                    <Plus className="h-3.75 w-3.75" />
                  </span>
                  새 보고서
                </button>
                <div className="mx-1 my-1.5 h-px bg-zinc-100" />
                <div className="px-2.25 pb-1.25 pt-0.5 text-[10px] font-extrabold uppercase tracking-wide text-zinc-400">
                  기존 보고서 불러오기
                </div>
                {reports.length === 0 ? (
                  <div className="px-2.25 py-2.5 text-[12.5px] font-semibold text-zinc-400">
                    기존 보고서 없음
                  </div>
                ) : (
                  reports.map((r) => {
                    const on = r.reportId === loadedReportId;
                    return (
                      <button
                        key={r.reportId}
                        type="button"
                        onClick={() => handleSelect(r.reportId)}
                        className={cn(
                          "flex w-full items-center gap-2.5 rounded-[9px] p-2.25 text-left transition",
                          on ? "bg-violet-50" : "hover:bg-zinc-100",
                        )}
                      >
                        <span
                          className={cn(
                            "grid h-7 w-7 shrink-0 place-items-center rounded-lg",
                            on
                              ? "bg-white text-violet-700"
                              : "bg-zinc-100 text-zinc-500",
                          )}
                        >
                          <FileText className="h-3.75 w-3.75" />
                        </span>
                        <span className="min-w-0 flex-1">
                          <span
                            className={cn(
                              "block truncate text-[13px] font-bold",
                              on ? "text-violet-700" : "text-zinc-900",
                            )}
                          >
                            {r.title || "제목 없는 보고서"}
                          </span>
                          <span className="mt-px block text-[11px] font-semibold text-zinc-400">
                            블록 {r.blockCount}개
                          </span>
                        </span>
                        {on && <Check className="h-4 w-4 shrink-0 text-violet-700" />}
                      </button>
                    );
                  })
                )}
              </div>
            )}
          </div>

          <button
            type="button"
            onClick={handleNew}
            title="새 보고서"
            className="grid h-10 w-10 shrink-0 place-items-center rounded-[10px] border border-zinc-200 bg-white text-zinc-500 transition hover:border-violet-500 hover:bg-violet-50 hover:text-violet-700"
          >
            <Plus className="h-4.5 w-4.5" />
          </button>
          <button
            type="button"
            onClick={closePanel}
            title="패널 닫기"
            className="grid h-10 w-10 shrink-0 place-items-center rounded-[10px] text-zinc-400 transition hover:bg-zinc-100 hover:text-zinc-900"
          >
            <X className="h-4.5 w-4.5" />
          </button>
        </div>

        {/* 보고서 제목 + 기초분석 가져오기 */}
        <div className="shrink-0 space-y-2.5 border-b border-zinc-200 px-4 py-3">
          <div>
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
        </div>

        {count === 0 ? (
          /* empty state — 새 보고서(기초분석 가져오기) / 기존 보고서 사용(드롭다운) */
          <div className="flex flex-1 flex-col items-center justify-center gap-3.75 px-7.5 text-center">
            <span className="grid h-14 w-14 place-items-center rounded-2xl bg-zinc-100 text-zinc-400">
              <FileText className="h-7 w-7" />
            </span>
            <h4 className="text-[15.5px] font-extrabold text-zinc-900">
              보고서가 선택되지 않았습니다
            </h4>
            <p className="max-w-62.5 text-[12.5px] leading-relaxed text-zinc-400">
              새 보고서는 기초분석 결과를 기본 블록으로 가져옵니다. 기존 보고서를
              불러와 이어서 편집할 수도 있습니다.
            </p>
            <div className="mt-1 flex w-full max-w-60 flex-col gap-2.25">
              <button
                type="button"
                onClick={onNewReport}
                disabled={templateLoading}
                className="inline-flex h-10.5 items-center justify-center gap-1.75 rounded-xl bg-violet-600 text-[13.5px] font-bold text-white transition hover:bg-violet-700 disabled:opacity-60"
              >
                {templateLoading ? (
                  <Loader2 className="h-4 w-4 animate-spin" />
                ) : (
                  <Plus className="h-4 w-4" />
                )}
                새 보고서 만들기
              </button>
              <button
                type="button"
                onClick={() => setMenuOpen(true)}
                disabled={reportsLoading || reports.length === 0}
                className="inline-flex h-10.5 items-center justify-center gap-1.75 rounded-xl border border-zinc-200 bg-white text-[13.5px] font-bold text-zinc-600 transition hover:border-zinc-300 hover:text-zinc-900 disabled:opacity-50"
              >
                <FileText className="h-4 w-4" />
                기존 보고서 사용
              </button>
            </div>
          </div>
        ) : (
          <>
            {/* card list */}
            <div
              ref={scrollRef}
              onDragOver={(e) => {
                if (dragIndex === null) return;
                updateAutoScroll(e.clientY);
              }}
              className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-3.5"
            >
              {staged.map((block, index) => (
                <ReportPanelCard
                  key={block.uid}
                  block={block}
                  onTitleChange={(v) => setTitle(block.uid, v)}
                  onNoteChange={(v) => setNote(block.uid, v)}
                  onRemove={() => remove(block.uid)}
                  collapsed={!expandedIds.has(block.uid)}
                  onToggle={() => toggleExpand(block.uid)}
                  isDragging={dragIndex === index}
                  dropHint={dropHintFor(index)}
                  onDragStartCard={() => setDragIndex(index)}
                  onDragOverCard={(after) => setDropTarget({ index, after })}
                  onDropCard={handleDrop}
                  onDragEndCard={() => {
                    setDragIndex(null);
                    setDropTarget(null);
                    stopAutoScroll();
                  }}
                />
              ))}
            </div>

            {/* footer */}
            <div className="flex shrink-0 gap-2.5 border-t border-zinc-200 px-3.5 py-3">
              <button
                type="button"
                onClick={clearAll}
                disabled={saving}
                title="전체 비우기"
                className="grid h-10 w-11 place-items-center rounded-xl border border-zinc-200 bg-white text-zinc-500 transition hover:border-zinc-300 hover:text-zinc-900 disabled:opacity-50"
              >
                <X className="h-4 w-4" />
              </button>
              <button
                type="button"
                onClick={onSave}
                disabled={saving}
                className="flex h-10 flex-1 items-center justify-center gap-1.75 rounded-xl bg-violet-600 text-[13.5px] font-bold text-white transition hover:bg-violet-700 disabled:opacity-60"
              >
                {saving ? (
                  <>
                    <Loader2 className="h-4 w-4 animate-spin" />
                    {loadedReportId ? "저장 중…" : "만드는 중…"}
                  </>
                ) : (
                  <>
                    <FileText className="h-4 w-4" />
                    {loadedReportId ? "보고서 저장" : "보고서 만들기"}
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
