// 선택된 블록의 속성 편집 팝오버. 선택 카드 옆(우/좌)에 떠서 표시 제목·해석 문구·
// 표시 옵션(원질문/상세/분석계획)·너비·삭제를 제어한다. 스크롤/리사이즈 시 위치 재계산.
import { useLayoutEffect, useRef, useState } from "react";
import {
  ClipboardCheck,
  MessageSquare,
  Table2,
  Trash2,
  X,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { BlockOpts, LibraryItem, ReportBlock } from "../models/editor";

const PANEL_W = 300;

function Switch({
  on,
  disabled,
  onClick,
}: {
  on: boolean;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      disabled={disabled}
      onClick={onClick}
      className={cn(
        "relative ml-auto h-5.5 w-9.5 shrink-0 rounded-full transition-colors",
        on ? "bg-violet-600" : "bg-zinc-300",
        disabled && "opacity-40",
      )}
    >
      <i
        className={cn(
          "absolute left-0.5 top-0.5 h-4.5 w-4.5 rounded-full bg-white shadow transition-transform",
          on && "translate-x-4",
        )}
      />
    </button>
  );
}

function OptRow({
  icon,
  label,
  on,
  disabled,
  onToggle,
}: {
  icon: React.ReactNode;
  label: string;
  on: boolean;
  disabled?: boolean;
  onToggle: () => void;
}) {
  return (
    <div className="flex items-center gap-2.5 border-b border-zinc-100 py-2.25 text-[13px] font-medium text-zinc-900 last:border-0">
      <span className="text-zinc-400">{icon}</span>
      {label}
      <Switch on={on} disabled={disabled} onClick={onToggle} />
    </div>
  );
}

function Label({ children }: { children: React.ReactNode }) {
  return (
    <div className="mb-1.75 mt-3.5 text-[11px] font-extrabold uppercase tracking-wide text-zinc-400 first:mt-0">
      {children}
    </div>
  );
}

export function BlockPopover({
  block,
  lib,
  onClose,
  onSetTitle,
  onSetInterp,
  onToggleOpt,
  onResetWidth,
  onDelete,
}: {
  block: ReportBlock;
  lib: LibraryItem;
  onClose: () => void;
  onSetTitle: (title: string) => void;
  onSetInterp: (interp: string) => void;
  onToggleOpt: (key: keyof BlockOpts) => void;
  onResetWidth: () => void;
  onDelete: () => void;
}) {
  const panelRef = useRef<HTMLDivElement>(null);
  const [pos, setPos] = useState<{
    left: number;
    top: number;
    arrow: "left" | "right" | null;
    arrowTop: number;
  } | null>(null);

  useLayoutEffect(() => {
    const compute = () => {
      const card = document.querySelector(
        `[data-card="${block.uid}"]`,
      ) as HTMLElement | null;
      const panel = panelRef.current;
      if (!card || !panel) return;
      const r = card.getBoundingClientRect();
      const ph = panel.offsetHeight;
      const gap = 14;
      const leftBound = 80;
      let left: number;
      let arrow: "left" | "right" | null;
      if (r.right + gap + PANEL_W <= window.innerWidth - 12) {
        left = r.right + gap;
        arrow = "right";
      } else if (r.left - gap - PANEL_W >= leftBound) {
        left = r.left - PANEL_W - gap;
        arrow = "left";
      } else {
        left = window.innerWidth - PANEL_W - 12;
        arrow = null;
      }
      const top = Math.max(12, Math.min(r.top, window.innerHeight - ph - 12));
      const arrowTop = Math.max(14, Math.min(r.top - top + 22, ph - 26));
      setPos({ left, top, arrow, arrowTop });
    };
    compute();
    window.addEventListener("resize", compute);
    window.addEventListener("scroll", compute, true);
    return () => {
      window.removeEventListener("resize", compute);
      window.removeEventListener("scroll", compute, true);
    };
  }, [block.uid, block.width]);

  const title = block.title != null ? block.title : lib.title;

  return (
    <>
      {/* 바깥 클릭 닫기 */}
      <div className="fixed inset-0 z-40" onClick={onClose} />
      <div
        ref={panelRef}
        style={{
          left: pos?.left ?? -9999,
          top: pos?.top ?? -9999,
          width: PANEL_W,
        }}
        className="fixed z-50 rounded-2xl border border-zinc-200 bg-white p-4 shadow-2xl"
      >
        {pos?.arrow && (
          <span
            className={cn(
              "absolute h-3 w-3 rotate-45 border border-zinc-200 bg-white",
              pos.arrow === "right"
                ? "-left-1.5 border-b-0 border-r-0"
                : "-right-1.5 border-l-0 border-t-0",
            )}
            style={{ top: pos.arrowTop }}
          />
        )}

        <div className="mb-3.5 flex items-center gap-2">
          <b className="text-[13.5px] font-extrabold text-zinc-900">블록 속성</b>
          <button
            onClick={onClose}
            className="ml-auto grid h-6.5 w-6.5 place-items-center rounded-lg text-zinc-400 transition hover:bg-zinc-100 hover:text-zinc-900"
          >
            <X className="h-3.75 w-3.75" />
          </button>
        </div>

        <Label>표시 제목</Label>
        <input
          value={title}
          onChange={(e) => onSetTitle(e.target.value)}
          className="w-full rounded-lg border border-zinc-200 px-2.75 py-2.25 text-[13px] text-zinc-900 outline-none transition focus:border-violet-500 focus:ring-3 focus:ring-violet-100"
        />

        <Label>해석 문구</Label>
        <textarea
          value={block.interp}
          onChange={(e) => onSetInterp(e.target.value)}
          placeholder="이 결과에 대한 해석을 적어주세요"
          className="h-19 w-full resize-none rounded-lg border border-zinc-200 px-2.75 py-2.25 text-[13px] leading-relaxed text-zinc-900 outline-none transition focus:border-violet-500 focus:ring-3 focus:ring-violet-100"
        />

        <Label>표시 옵션</Label>
        <OptRow
          icon={<MessageSquare className="h-3.75 w-3.75" />}
          label="원 질문 표시"
          on={block.opts.q}
          onToggle={() => onToggleOpt("q")}
        />
        <OptRow
          icon={<Table2 className="h-3.75 w-3.75" />}
          label={lib.detail ? "상세 데이터 포함" : "상세 데이터 포함 (없음)"}
          on={block.opts.detail}
          disabled={!lib.detail}
          onToggle={() => onToggleOpt("detail")}
        />
        <OptRow
          icon={<ClipboardCheck className="h-3.75 w-3.75" />}
          label="분석 계획 포함"
          on={block.opts.plan}
          onToggle={() => onToggleOpt("plan")}
        />

        <Label>너비</Label>
        <div className="flex items-center gap-2.5">
          <span className="text-[13px] font-bold tabular-nums text-zinc-900">
            {block.width ? `${block.width}px` : "전체 너비"}
          </span>
          <button
            disabled={!block.width}
            onClick={onResetWidth}
            className="ml-auto rounded-lg border border-zinc-200 px-2.75 py-1.5 text-xs font-bold text-zinc-600 transition enabled:hover:border-zinc-300 enabled:hover:text-zinc-900 disabled:opacity-40"
          >
            전체 너비로
          </button>
        </div>
        <div className="mt-1.75 text-[11.5px] text-zinc-400">
          블록 오른쪽 모서리를 드래그해 조절
        </div>

        <button
          onClick={onDelete}
          className="mt-4 flex w-full items-center justify-center gap-1.75 rounded-lg border border-red-100 bg-red-50 py-2.25 text-[13px] font-bold text-red-600 transition hover:brightness-97"
        >
          <Trash2 className="h-3.5 w-3.5" />
          블록 삭제
        </button>
      </div>
    </>
  );
}
