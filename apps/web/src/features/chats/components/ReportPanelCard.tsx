import { ChevronDown, GripVertical, Pencil, X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";
import ChartView from "./ChartView";
import DisplayTable from "./DisplayTable";
import EvidenceCardList from "./EvidenceCardList";
import MetricCompareView from "./MetricCompareView";
import EditableText from "./EditableText";

// 시안 「분석 채팅 - 보고서 패널」 .scard — 패널에 쌓인 결과 카드 1개.
// 드래그 핸들 + 제거 / 편집 제목 / 결과 본문 / 편집 메모.

// 메인 결과 선택(metric > evidence > chart > table) — MessageBubble과 동일 규칙.
function ResultBody({ message }: { message: ChatMessage }) {
  if (message.metric) return <MetricCompareView metric={message.metric} />;
  if (message.evidence) return <EvidenceCardList evidence={message.evidence} />;
  if (message.chart) return <ChartView chart={message.chart} />;
  if (message.display) return <DisplayTable display={message.display} />;
  return <p className="text-xs text-zinc-400">표시할 결과가 없습니다</p>;
}

export type DropHint = "before" | "after" | null;

interface ReportPanelCardProps {
  message: ChatMessage;
  title: string;
  note: string;
  onTitleChange: (value: string) => void;
  onNoteChange: (value: string) => void;
  onRemove: () => void;
  // accordion: 접힘 상태 + 토글. 접히면 제목 행만 보인다.
  collapsed: boolean;
  onToggle: () => void;
  // 드래그 정렬
  isDragging: boolean;
  dropHint: DropHint;
  onDragStartCard: () => void;
  onDragOverCard: (after: boolean) => void;
  onDropCard: () => void;
  onDragEndCard: () => void;
}

export default function ReportPanelCard({
  message,
  title,
  note,
  onTitleChange,
  onNoteChange,
  onRemove,
  collapsed,
  onToggle,
  isDragging,
  dropHint,
  onDragStartCard,
  onDragOverCard,
  onDropCard,
  onDragEndCard,
}: ReportPanelCardProps) {
  return (
    <div
      onDragOver={(e) => {
        e.preventDefault();
        const r = e.currentTarget.getBoundingClientRect();
        onDragOverCard(e.clientY >= r.top + r.height / 2);
      }}
      onDrop={(e) => {
        e.preventDefault();
        onDropCard();
      }}
      className={cn(
        "rounded-xl border border-zinc-200 bg-white shadow-sm transition",
        isDragging && "opacity-40",
        dropHint === "before" && "shadow-[0_-2px_0_0_#7c3aed]",
        dropHint === "after" && "shadow-[0_2px_0_0_#7c3aed]",
      )}
    >
      {/* top row: 드래그 핸들 + 접기/펼치기 + 제목 + 제거 */}
      <div className="flex items-center gap-2 px-3 pt-2.5 pb-1.5">
        <span
          draggable
          onDragStart={onDragStartCard}
          onDragEnd={onDragEndCard}
          title="드래그하여 순서 변경"
          className="grid h-5.5 w-5.5 cursor-grab place-items-center rounded-md text-zinc-400 hover:bg-zinc-100 hover:text-zinc-600 active:cursor-grabbing"
        >
          <GripVertical className="h-3.75 w-3.75" />
        </span>
        <button
          type="button"
          onClick={onToggle}
          title={collapsed ? "펼치기" : "접기"}
          aria-expanded={!collapsed}
          className="grid h-5.5 w-5.5 shrink-0 place-items-center rounded-md text-zinc-400 transition hover:bg-zinc-100 hover:text-zinc-600"
        >
          <ChevronDown
            className={cn(
              "h-4 w-4 transition-transform",
              collapsed && "-rotate-90",
            )}
          />
        </button>
        {/* editable title (편집 가능한 카드 제목) */}
        <div className="group flex min-w-0 flex-1 items-start gap-1.5">
          <EditableText
            value={title}
            onChange={onTitleChange}
            ariaLabel="카드 제목"
            className="min-w-0 flex-1 cursor-text truncate rounded-md py-0.5 text-[13.5px] font-bold leading-snug text-zinc-900 hover:bg-zinc-100 focus:bg-white focus:ring-2 focus:ring-violet-200"
          />
          <Pencil className="mt-1 h-3.5 w-3.5 shrink-0 text-zinc-300 opacity-0 transition group-hover:opacity-100" />
        </div>
        <button
          type="button"
          onClick={onRemove}
          title="제거"
          className="grid h-6 w-6 shrink-0 place-items-center rounded-md text-zinc-400 transition hover:bg-red-50 hover:text-red-600"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      {/* 접힌 카드는 제목 행만 — 결과 본문·메모는 펼쳤을 때만 렌더 */}
      {!collapsed && (
        <>
          {/* rendered result — 결과 컴포넌트의 자체 박스만(중첩 래퍼 제거) */}
          <div className="px-3">
            <ResultBody message={message} />
          </div>

          {/* editable note */}
          <EditableText
            value={note}
            onChange={onNoteChange}
            multiline
            placeholder="메모를 추가…"
            ariaLabel="카드 메모"
            className="m-3 min-h-8.5 rounded-lg border border-dashed border-zinc-300 px-2.75 py-2 text-[12.5px] leading-relaxed text-zinc-600 transition hover:border-zinc-400 focus:border-solid focus:border-violet-500 focus:bg-white focus:ring-2 focus:ring-violet-100"
          />
        </>
      )}
    </div>
  );
}
