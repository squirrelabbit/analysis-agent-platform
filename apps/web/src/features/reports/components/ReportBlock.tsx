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
  onSetHeight,
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
  onSetHeight: (uid: string, height: number | null) => void;
}) {
  const [dragSpan, setDragSpan] = useState<number | null>(null);
  const [dragHeight, setDragHeight] = useState<number | null>(null);

  const isEdit = mode === "edit";
  const title = block.title != null ? block.title : lib.title;
  const span = dragSpan ?? block.span;
  // 드래그 중이면 실시간 값, 아니면 저장된 height(null=자동).
  const minHeight = dragHeight ?? block.height;

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

  // 하단 모서리 드래그로 카드 높이(minHeight) 조절. 너비 핸들과 대칭.
  const startResizeV = (e: React.PointerEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const card = (e.currentTarget as HTMLElement).closest(
      "[data-card]",
    ) as HTMLElement | null;
    // 줄어들 수 있는 바닥 = 이 카드의 순수 콘텐츠 높이. 줄 정렬(stretch)·minHeight를
    // 잠깐 무력화하고 측정 → 옆에 큰 블록이 있어도 콘텐츠까지는 줄일 수 있다(스크롤 없음).
    let floor = 80;
    if (card) {
      const wrapper = card.parentElement;
      const prevSelf = wrapper?.style.alignSelf ?? "";
      const prevMin = card.style.minHeight;
      const prevH = card.style.height;
      if (wrapper) wrapper.style.alignSelf = "start";
      card.style.minHeight = "0px";
      card.style.height = "auto";
      floor = Math.round(card.getBoundingClientRect().height);
      card.style.height = prevH;
      card.style.minHeight = prevMin;
      if (wrapper) wrapper.style.alignSelf = prevSelf;
    }
    const startH = card?.getBoundingClientRect().height ?? 200;
    const startY = e.clientY;
    const handle = e.currentTarget as HTMLElement;
    try {
      handle.setPointerCapture(e.pointerId);
    } catch {
      /* ignore */
    }
    document.body.style.cursor = "ns-resize";
    document.body.style.userSelect = "none";
    let committed: number | null = block.height;

    const move = (ev: PointerEvent) => {
      // 콘텐츠 바닥 아래로는 클램프 → 배지 px도 바닥에서 멈춰 더 안 줄어듦이 보인다.
      const h = Math.max(floor, Math.round(startH + (ev.clientY - startY)));
      committed = h;
      setDragHeight(h);
    };
    const up = () => {
      handle.removeEventListener("pointermove", move);
      handle.removeEventListener("pointerup", up);
      handle.removeEventListener("pointercancel", up);
      document.body.style.cursor = "";
      document.body.style.userSelect = "";
      setDragHeight(null);
      // 끌어 정한 높이를 그대로 유지(자동 정렬에서 빠져 독립). 자동 복귀는 "자동 높이로" 버튼.
      onSetHeight(block.uid, committed);
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
      className={cn(
        "group/block relative min-w-0",
        selected && "z-10",
        // 높이를 직접 지정한 카드만 self-start로 빼 줄 높이에서 독립(옆보다 작아질 수 있음).
        // 미지정 카드는 부모 items-stretch로 줄 최고 높이에 자동 정렬.
        minHeight != null && "self-start",
      )}
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
        // 높이 드래그 값(있으면) → minHeight. null이면 콘텐츠 높이(자동).
        style={minHeight != null ? { minHeight } : undefined}
        className={cn(
          "relative w-full rounded-2xl border bg-white px-5.5 py-5 shadow-sm transition",
          // height 미지정 카드만 h-full로 줄 높이를 채워 정렬. 지정 카드는 self-start라 제외.
          minHeight == null && "h-full",
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

        {/* 높이 리사이즈 핸들 (편집 모드) — 하단 모서리 */}
        {isEdit && (
          <div
            onPointerDown={startResizeV}
            title="드래그해 높이 조절"
            className={cn(
              "absolute -bottom-1.25 left-0 right-0 z-3 flex h-3.5 cursor-ns-resize items-center justify-center transition",
              dragHeight != null
                ? "opacity-100"
                : "opacity-0 group-hover/block:opacity-100",
            )}
          >
            <span
              className={cn(
                "h-1 w-11.5 rounded-full",
                dragHeight != null ? "bg-violet-500" : "bg-zinc-300",
              )}
            />
          </div>
        )}

        {/* 리사이즈 중 현재 높이(px) 표시 */}
        {isEdit && dragHeight != null && (
          <div className="absolute -bottom-3 left-1/2 z-10 -translate-x-1/2 rounded-full bg-violet-600 px-2.5 py-0.5 text-[11px] font-bold text-white shadow">
            {dragHeight}px
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
