import { BarChart3, GripVertical, Pencil, Quote, Sigma, Table2, X } from "lucide-react";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";
import { cardType, type CardType } from "../hooks/useReportPanel";
import ChartView from "./ChartView";
import DisplayTable from "./DisplayTable";
import EvidenceCardList from "./EvidenceCardList";
import MetricCompareView from "./MetricCompareView";
import EditableText from "./EditableText";

// 시안 「분석 채팅 - 보고서 패널」 .scard — 패널에 쌓인 결과 카드 1개.
// 드래그 핸들 + 순번 + 타입 배지 + 제거 / 편집 제목 / 결과 본문 / 편집 메모.

const TYPE_META: Record<
  CardType,
  { label: string; Icon: typeof BarChart3; badge: string }
> = {
  metric: { label: "지표", Icon: Sigma, badge: "bg-violet-50 text-violet-700" },
  chart: { label: "차트", Icon: BarChart3, badge: "bg-blue-50 text-blue-600" },
  quote: { label: "원문", Icon: Quote, badge: "bg-amber-50 text-amber-600" },
  table: { label: "표", Icon: Table2, badge: "bg-emerald-50 text-emerald-700" },
};

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
  index: number;
  title: string;
  note: string;
  onTitleChange: (value: string) => void;
  onNoteChange: (value: string) => void;
  onRemove: () => void;
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
  index,
  title,
  note,
  onTitleChange,
  onNoteChange,
  onRemove,
  isDragging,
  dropHint,
  onDragStartCard,
  onDragOverCard,
  onDropCard,
  onDragEndCard,
}: ReportPanelCardProps) {
  const type = cardType(message);
  const { label, Icon, badge } = TYPE_META[type];

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
      {/* top row */}
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
        <span className="grid h-5.5 w-5.5 place-items-center rounded-md bg-zinc-900 text-[12px] font-extrabold text-white">
          {index + 1}
        </span>
        <span
          className={cn(
            "inline-flex items-center gap-1 rounded-full px-1.75 py-0.5 text-[10.5px] font-extrabold",
            badge,
          )}
        >
          <Icon className="h-2.75 w-2.75" />
          {label}
        </span>
        <button
          type="button"
          onClick={onRemove}
          title="제거"
          className="ml-auto grid h-6 w-6 place-items-center rounded-md text-zinc-400 transition hover:bg-red-50 hover:text-red-600"
        >
          <X className="h-3.5 w-3.5" />
        </button>
      </div>

      {/* editable title */}
      <div className="group flex items-start gap-1.5 px-3">
        <EditableText
          value={title}
          onChange={onTitleChange}
          ariaLabel="카드 제목"
          className="min-w-0 flex-1 cursor-text rounded-md px-1.5 py-0.5 text-[13.5px] font-bold leading-snug text-zinc-900 hover:bg-zinc-100 focus:bg-white focus:ring-2 focus:ring-violet-200"
        />
        <Pencil className="mt-1 h-3.5 w-3.5 shrink-0 text-zinc-300 opacity-0 transition group-hover:opacity-100" />
      </div>

      {/* full rendered result */}
      <div className="mx-3 mt-2 overflow-x-auto rounded-[10px] border border-zinc-200 bg-zinc-50/60 px-3 py-2.5">
        <ResultBody message={message} />
      </div>

      {/* editable note */}
      <EditableText
        value={note}
        onChange={onNoteChange}
        multiline
        placeholder="메모를 추가…"
        ariaLabel="카드 메모"
        className="m-3 min-h-[34px] rounded-lg border border-dashed border-zinc-300 px-2.75 py-2 text-[12.5px] leading-relaxed text-zinc-600 transition hover:border-zinc-400 focus:border-solid focus:border-violet-500 focus:bg-white focus:ring-2 focus:ring-violet-100"
      />
    </div>
  );
}
