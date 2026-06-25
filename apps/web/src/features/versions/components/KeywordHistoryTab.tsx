import { DataTable, type Column } from "./DataTable";
import {
  useKeywordDictionaryEvents,
} from "../hooks/build.query";
import type {
  KeywordDictionaryEvent,
  KeywordRulePayload,
} from "../models/build";

const EVENT_LABEL: Record<string, string> = {
  add: "추가",
  update: "수정",
  deactivate: "해제",
  reactivate: "재활성",
};

const fmtDateTime = (iso: string) =>
  iso ? iso.replace("T", " ").slice(0, 16) : "-";

// before/after payload에서 "수제맥주 → 맥주" / "가격 제외" 식 내용 문구 합성.
const describe = (p?: KeywordRulePayload): string => {
  if (!p || !p.sourceTerm) return "-";
  if (p.ruleType === "synonym" && p.targetTerm) {
    return `${p.sourceTerm} → ${p.targetTerm}`;
  }
  return `${p.sourceTerm} 제외`;
};

// 변경 이력 탭 (silverone 2026-06-25) — append-only event 로그(최신순).
export default function KeywordHistoryTab() {
  const { data: events = [], isLoading } = useKeywordDictionaryEvents();

  const columns: Column<KeywordDictionaryEvent>[] = [
    {
      header: "일시",
      cell: (e) => (
        <td className="px-3 py-2 text-slate-500 whitespace-nowrap">
          {fmtDateTime(e.createdAt)}
        </td>
      ),
    },
    {
      header: "작업",
      cell: (e) => (
        <td className="px-3 py-2">{EVENT_LABEL[e.eventType] ?? e.eventType}</td>
      ),
    },
    {
      header: "내용",
      cell: (e) => (
        <td className="px-3 py-2 font-medium">{describe(e.after ?? e.before)}</td>
      ),
    },
    {
      header: "사유",
      cell: (e) => (
        <td className="px-3 py-2 text-slate-600">{e.reason || "-"}</td>
      ),
    },
  ];

  return (
    <DataTable
      columns={columns}
      items={events}
      rowKey={(e) => e.id}
      title={
        <span className="text-sm font-semibold">
          변경 이력 <span className="text-slate-400">{events.length}</span>
        </span>
      }
      emptyText="변경 이력이 없습니다."
      page={1}
      totalPages={1}
      totalCount={events.length}
      onPageChange={() => {}}
      loading={isLoading}
    />
  );
}
