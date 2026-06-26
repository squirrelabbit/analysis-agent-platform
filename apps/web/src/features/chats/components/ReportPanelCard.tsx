import { useMemo } from "react";
import { BarChart3, ChevronDown, GripVertical, Pencil, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { projectResult } from "@/features/reports/models/result";
import TemplateSection from "@/features/reports/components/TemplateSection";
import type { ReportBlock } from "@/features/reports/models";
import ChartView from "./ChartView";
import DisplayTable from "./DisplayTable";
import EvidenceCardList from "./EvidenceCardList";
import MetricCompareView from "./MetricCompareView";
import EditableText from "./EditableText";

// 패널에 쌓인 블록 카드 1개. result(분석 결과) / section(기초분석 템플릿) 둘 다 렌더.
// 드래그 핸들 + 접기/펼치기(accordion) + 편집 제목 + 결과 본문 + 편집 메모.

// result 본문 — 메인 결과 선택(metric > evidence > chart > table), 채팅과 동일.
function ResultBody({ block }: { block: ReportBlock }) {
  const r = useMemo(
    () => (block.result ? projectResult(block.result) : undefined),
    [block.result],
  );
  if (!r) return <p className="text-xs text-zinc-400">표시할 결과가 없습니다</p>;
  if (r.metric) return <MetricCompareView metric={r.metric} />;
  if (r.evidence) return <EvidenceCardList evidence={r.evidence} />;
  if (r.chart) return <ChartView chart={r.chart} />;
  if (r.display) return <DisplayTable display={r.display} />;
  return <p className="text-xs text-zinc-400">표시할 결과가 없습니다</p>;
}

export type DropHint = "before" | "after" | null;

interface ReportPanelCardProps {
  block: ReportBlock;
  onTitleChange: (value: string) => void;
  onNoteChange: (value: string) => void;
  onRemove: () => void;
  collapsed: boolean;
  onToggle: () => void;
  isDragging: boolean;
  dropHint: DropHint;
  onDragStartCard: () => void;
  onDragOverCard: (after: boolean) => void;
  onDropCard: () => void;
  onDragEndCard: () => void;
}

export default function ReportPanelCard({
  block,
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
  const isSection = block.kind === "section";
  const defaultTitle = isSection
    ? block.section?.defaultTitle || "섹션"
    : block.result?.defaultTitle || "분석 결과";
  const title = block.title != null ? block.title : defaultTitle;

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
        {isSection && (
          <span
            title="기초분석 섹션"
            className="grid h-5.5 w-5.5 shrink-0 place-items-center rounded-md bg-violet-50 text-violet-600"
          >
            <BarChart3 className="h-3.5 w-3.5" />
          </span>
        )}
        {/* editable title */}
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

      {/* 접힌 카드는 제목 행만 — 본문·메모는 펼쳤을 때만 렌더 */}
      {!collapsed && (
        <>
          <div className="px-3">
            {isSection ? (
              block.section && <TemplateSection rows={block.section.rows} />
            ) : (
              <ResultBody block={block} />
            )}
          </div>

          {/* editable note */}
          <EditableText
            value={block.interp}
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
