// 보고서 캔버스의 단일 블록. 그립(드래그 정렬) + 카드(너비 리사이즈) + 속성 편집 버튼 +
// 원 질문 칩 + 제목/부제 + viz + 해석 문구 + 상세/분석계획 폴드.
import { useState, type RefObject } from "react";
import { GripVertical, MessageSquare, Pencil } from "lucide-react";
import { cn } from "@/lib/utils";
import {
  GRID_COLS,
  snapSpan,
  spanLabel,
  type LibraryItem,
  type ReportBlock as Block,
  type ReportMode,
} from "../models/editor";
import ChartView from "@/features/chats/components/ChartView";
import CollapsibleTable from "@/features/chats/components/CollapsibleTable";
import DisplayTable from "@/features/chats/components/DisplayTable";
import EvidenceCardList from "@/features/chats/components/EvidenceCardList";
import MetricCompareView from "@/features/chats/components/MetricCompareView";
import PlanPanel from "@/features/chats/components/PlanPanel";

export function ReportBlock({
  block,
  lib,
  index,
  mode,
  selected,
  sheetRef,
  onEdit,
  onGripDragStart,
  onGripDragEnd,
  onSetSpan,
}: {
  block: Block;
  lib: LibraryItem;
  index: number;
  mode: ReportMode;
  selected: boolean;
  sheetRef: RefObject<HTMLDivElement | null>;
  onEdit: (uid: string) => void;
  onGripDragStart: (uid: string) => void;
  onGripDragEnd: () => void;
  onSetSpan: (uid: string, span: number) => void;
}) {
  const [dragSpan, setDragSpan] = useState<number | null>(null);

  const isEdit = mode === "edit";
  const title = block.title != null ? block.title : lib.title;
  const span = dragSpan ?? block.span;

  // 메인 결과 1개 선택(채팅과 동일): metric > evidence > chart > table.
  const { result } = lib;
  const metricMain = !!result.metric;
  const evidenceMain = !metricMain && !!result.evidence;
  const chartMain = !metricMain && !evidenceMain && !!result.chart;
  const tableMain = !metricMain && !evidenceMain && !chartMain && !!result.display;
  const hasNonTableMain = metricMain || evidenceMain || chartMain;

  const startResize = (e: React.PointerEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const containerW = sheetRef.current?.getBoundingClientRect().width ?? 760;
    const gap = 12; // gap-3
    // 컬럼+gap 한 단위 폭. span s 블록 폭 = s*moduleW - gap.
    const moduleW = (containerW + gap) / GRID_COLS;
    const card = (e.currentTarget as HTMLElement).closest(
      "[data-card]",
    ) as HTMLElement | null;
    const startW = card?.getBoundingClientRect().width ?? containerW;
    const startX = e.clientX;
    const handle = e.currentTarget as HTMLElement;
    try {
      handle.setPointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
    document.body.style.cursor = "ew-resize";
    document.body.style.userSelect = "none";
    let committed = block.span;

    const move = (ev: PointerEvent) => {
      const w = startW + (ev.clientX - startX);
      // 폭 → 컬럼 수: w = s*moduleW - gap → s = (w + gap)/moduleW. 후보로 스냅.
      const s = snapSpan((w + gap) / moduleW);
      committed = s;
      setDragSpan(s);
    };
    const up = () => {
      handle.removeEventListener("pointermove", move);
      handle.removeEventListener("pointerup", up);
      handle.removeEventListener("pointercancel", up);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      setDragSpan(null);
      onSetSpan(block.uid, committed);
    };
    handle.addEventListener("pointermove", move);
    handle.addEventListener("pointerup", up);
    handle.addEventListener("pointercancel", up);
  };

  return (
    <div
      // 12컬럼 그리드 아이템. newRow(또는 첫 블록)면 col 1에서 시작(새 줄), 아니면 앞 블록에
      // 이어 배치(나란히). 자동 packing 없이 명시적 나란히만 같은 줄을 공유한다.
      style={{
        gridColumn:
          index === 0 || block.newRow ? `1 / span ${span}` : `span ${span}`,
      }}
      className={cn("group/block relative min-w-0", selected && "z-10")}
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
        className={cn(
          "relative w-full rounded-2xl border bg-white px-5.5 py-5 shadow-sm transition",
          selected
            ? "border-violet-300 ring-3 ring-violet-100"
            : "border-zinc-100 group-hover/block:border-zinc-200",
        )}
      >
        {/* 리사이즈 중 현재 분수(½·⅓·⅔·전체) 표시 */}
        {isEdit && dragSpan != null && (
          <div className="absolute -top-3 left-1/2 z-10 -translate-x-1/2 rounded-full bg-violet-600 px-2.5 py-0.5 text-[11px] font-bold text-white shadow">
            {spanLabel(span)}
          </div>
        )}

        {/* 너비 리사이즈 핸들 (편집 모드) */}
        {isEdit && (
          <div
            onPointerDown={startResize}
            title="드래그해 너비 조절"
            className={cn(
              "absolute -right-1.25 top-0 bottom-0 z-3 flex w-3.5 cursor-ew-resize items-center justify-center transition",
              dragSpan != null
                ? "opacity-100"
                : "opacity-0 group-hover/block:opacity-100",
            )}
          >
            <span
              className={cn(
                "h-11.5 w-1 rounded-full",
                dragSpan != null ? "bg-violet-500" : "bg-zinc-300",
              )}
            />
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

        {/* 메인 결과 — 채팅 결과 뷰 카탈로그 재사용(metric>evidence>chart>table) */}
        <div className="mt-4">
          {metricMain && result.metric && (
            <MetricCompareView metric={result.metric} />
          )}
          {evidenceMain && result.evidence && (
            <EvidenceCardList evidence={result.evidence} />
          )}
          {chartMain && result.chart && <ChartView chart={result.chart} />}
          {tableMain && result.display && (
            <DisplayTable display={result.display} />
          )}
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

        {/* 메인이 표가 아니면 display는 상세 데이터 접이식으로(채팅과 동일) */}
        {block.opts.detail && hasNonTableMain && result.display && (
          <CollapsibleTable display={result.display} />
        )}

        {block.opts.plan && result.plan && <PlanPanel plan={result.plan} />}
      </div>
    </div>
  );
}
