// 보고서 캔버스의 단일 블록. 그립(드래그 정렬) + 카드(너비 리사이즈) + 속성 편집 버튼 +
// 원 질문 칩 + 제목/부제 + viz + 해석 문구 + 상세/분석계획 폴드.
import { useState, type RefObject } from "react";
import {
  ChevronDown,
  ClipboardCheck,
  GripVertical,
  MessageSquare,
  Pencil,
  Table2,
} from "lucide-react";
import { cn } from "@/lib/utils";
import type { LibraryItem, ReportBlock as Block, ReportMode } from "../models/editor";
import { Viz, VizGrid, VizPlan } from "./Viz";

const MIN_W = 340;

function Fold({
  icon,
  label,
  open,
  onToggle,
  children,
}: {
  icon: React.ReactNode;
  label: string;
  open: boolean;
  onToggle: () => void;
  children: React.ReactNode;
}) {
  const isOpen = open;
  return (
    <div className="mt-3.5 overflow-hidden rounded-xl border border-zinc-100">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center gap-2.25 bg-zinc-50/70 px-3.5 py-2.75 text-[13px] font-semibold text-zinc-600 transition-colors hover:bg-zinc-100/70"
      >
        <span className="text-zinc-400">{icon}</span>
        {label}
        <ChevronDown
          className={cn(
            "ml-auto h-4 w-4 text-zinc-400 transition-transform",
            isOpen && "rotate-180",
          )}
        />
      </button>
      {isOpen && (
        <div className="border-t border-zinc-100 p-3.5">{children}</div>
      )}
    </div>
  );
}

export function ReportBlock({
  block,
  lib,
  index,
  mode,
  selected,
  sheetRef,
  onSelect,
  onEdit,
  onGripDragStart,
  onGripDragEnd,
  onSetWidth,
}: {
  block: Block;
  lib: LibraryItem;
  index: number;
  mode: ReportMode;
  selected: boolean;
  sheetRef: RefObject<HTMLDivElement | null>;
  onSelect: (uid: string) => void;
  onEdit: (uid: string) => void;
  onGripDragStart: (uid: string) => void;
  onGripDragEnd: () => void;
  onSetWidth: (uid: string, width: number | null) => void;
}) {
  const [foldDetail, setFoldDetail] = useState(false);
  const [foldPlan, setFoldPlan] = useState(false);
  const [dragWidth, setDragWidth] = useState<number | null>(null);

  const isEdit = mode === "edit";
  const title = block.title != null ? block.title : lib.title;
  const width = dragWidth ?? block.width;

  const startResize = (e: React.PointerEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const sheetW = sheetRef.current?.getBoundingClientRect().width ?? 760;
    const card = (e.currentTarget as HTMLElement).closest(
      "[data-card]",
    ) as HTMLElement | null;
    const startW = card?.getBoundingClientRect().width ?? sheetW;
    const startX = e.clientX;
    const handle = e.currentTarget as HTMLElement;
    try {
      handle.setPointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
    document.body.style.cursor = "ew-resize";
    document.body.style.userSelect = "none";
    let committed: number | null = block.width;

    const move = (ev: PointerEvent) => {
      const w = Math.max(MIN_W, Math.min(sheetW, startW + (ev.clientX - startX)));
      const full = w >= sheetW - 6;
      committed = full ? null : Math.round(w);
      setDragWidth(full ? sheetW : Math.round(w));
    };
    const up = () => {
      handle.removeEventListener("pointermove", move);
      handle.removeEventListener("pointerup", up);
      handle.removeEventListener("pointercancel", up);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      setDragWidth(null);
      onSetWidth(block.uid, committed);
    };
    handle.addEventListener("pointermove", move);
    handle.addEventListener("pointerup", up);
    handle.addEventListener("pointercancel", up);
  };

  return (
    <div
      className={cn("group/block relative", selected && "z-10")}
      onClick={() => isEdit && onSelect(block.uid)}
    >
      {/* 드래그 그립 (편집 모드) */}
      {isEdit && (
        <div
          draggable
          onDragStart={(e) => {
            e.dataTransfer.effectAllowed = "move";
            try {
              e.dataTransfer.setData("text/plain", block.uid);
            } catch {
              /* ignore */
            }
            onGripDragStart(block.uid);
          }}
          onDragEnd={onGripDragEnd}
          className="absolute -left-6.5 top-1/2 grid h-10 w-6 -translate-y-1/2 cursor-grab place-items-center rounded-md text-zinc-300 opacity-0 transition hover:bg-zinc-100 hover:text-zinc-500 group-hover/block:opacity-100 active:cursor-grabbing"
          title="드래그해 순서 변경"
        >
          <GripVertical className="h-4 w-4" />
        </div>
      )}

      <div
        data-card={block.uid}
        data-idx={index}
        style={width ? { maxWidth: width } : undefined}
        className={cn(
          "relative rounded-2xl border bg-white px-5.5 py-5 shadow-sm transition",
          selected
            ? "border-violet-300 ring-3 ring-violet-100"
            : "border-zinc-100 group-hover/block:border-zinc-200",
        )}
      >
        {/* 너비 리사이즈 핸들 (편집 모드) */}
        {isEdit && (
          <div
            onPointerDown={startResize}
            title="드래그해 너비 조절"
            className="absolute -right-1.25 top-0 bottom-0 z-3 flex w-3.5 cursor-ew-resize items-center justify-center opacity-0 transition group-hover/block:opacity-100"
          >
            <span className="h-11.5 w-1 rounded-full bg-zinc-300" />
          </div>
        )}

        {/* 속성 편집 버튼 */}
        {isEdit && (
          <div className="absolute right-3 top-3 opacity-0 transition group-hover/block:opacity-100">
            <button
              onClick={(e) => {
                e.stopPropagation();
                onEdit(block.uid);
              }}
              title="속성 편집"
              className={cn(
                "grid h-7 w-7 place-items-center rounded-lg border transition",
                selected
                  ? "border-transparent bg-violet-50 text-violet-700"
                  : "border-zinc-200 bg-white text-zinc-500 hover:border-zinc-300 hover:text-zinc-900",
              )}
            >
              <Pencil className="h-3.75 w-3.75" />
            </button>
          </div>
        )}

        {block.opts.q && (
          <span className="mb-3.25 inline-flex items-center gap-1.75 rounded-full bg-zinc-100 px-3 py-1.5 text-[12.5px] font-semibold text-zinc-600">
            <MessageSquare className="h-3.25 w-3.25 text-zinc-400" />
            {lib.question}
          </span>
        )}

        <div className="text-[17px] font-bold tracking-tight text-zinc-900">
          {title}
        </div>
        <div className="mt-0.75 text-[12.5px] font-medium text-zinc-400">
          {lib.sub}
        </div>

        <div className="mt-4">
          <Viz lib={lib} />
        </div>

        {/* 해석 문구 */}
        {isEdit ? (
          <div
            className={cn(
              "mt-4 rounded-r-[10px] border-l-3 px-3.75 py-3 text-[13.5px] leading-relaxed",
              block.interp
                ? "border-violet-400 bg-violet-50 text-violet-700"
                : "border-zinc-300 bg-zinc-50 italic text-zinc-400",
            )}
          >
            {block.interp || "✎ 해석 문구를 추가하려면 블록을 선택하세요"}
          </div>
        ) : (
          block.interp && (
            <div className="mt-4 rounded-r-[10px] border-l-3 border-violet-400 bg-violet-50 px-3.75 py-3 text-[13.5px] leading-relaxed text-violet-700">
              {block.interp}
            </div>
          )
        )}

        {block.opts.detail && lib.detail && (
          <Fold
            icon={<Table2 className="h-3.5 w-3.5" />}
            label="상세 데이터"
            open={foldDetail}
            onToggle={() => setFoldDetail((v) => !v)}
          >
            <VizGrid d={lib.detail} />
          </Fold>
        )}

        {block.opts.plan && lib.plan.length > 0 && (
          <Fold
            icon={<ClipboardCheck className="h-3.5 w-3.5" />}
            label="분석 계획"
            open={foldPlan}
            onToggle={() => setFoldPlan((v) => !v)}
          >
            <VizPlan steps={lib.plan} />
          </Fold>
        )}
      </div>
    </div>
  );
}
