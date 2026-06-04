import { cn } from "@/lib/utils";
import type { ChatTableDisplay } from "../models";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";
import type { Taxonomy } from "@/features/taxonomy/models";

const MAX_VISIBLE_ROWS = 100;
// 20행 이하면 max-height 없이 자연 펼침, 그 이상은 360px 박스 + 내부 스크롤.
const COMPACT_ROWS_THRESHOLD = 20;

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

// aspect 컬럼의 영문 key 셀은 taxonomy 한글 label로 표시 (미매칭/미로딩 시 key 유지).
function renderCell(
  col: string,
  value: unknown,
  taxonomy: Taxonomy | undefined,
): string {
  if (col === ASPECT_FIELD && typeof value === "string") {
    return aspectLabelOf(taxonomy, value);
  }
  return formatCell(value);
}

export default function DisplayTable({ display }: { display: ChatTableDisplay }) {
  const { title, columns, rows } = display;
  // 조회 실패해도 renderCell이 key로 fallback하므로 화면은 동작한다.
  const { data: taxonomy } = useTaxonomy();
  const totalRows = rows.length;
  const visibleRows = rows.slice(0, MAX_VISIBLE_ROWS);
  const truncated = totalRows > MAX_VISIBLE_ROWS;
  const hasRows = totalRows > 0;
  const compact = totalRows <= COMPACT_ROWS_THRESHOLD;
  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      {(title || truncated) && (
        <div className="flex items-center justify-between gap-3 px-3 py-2 border-b border-zinc-100 text-xs font-medium text-zinc-600">
          <span>{title}</span>
          {truncated && (
            <span className="text-zinc-400 font-normal whitespace-nowrap">
              총 {totalRows.toLocaleString()}행 중 {MAX_VISIBLE_ROWS}행 표시
            </span>
          )}
        </div>
      )}
      <div className={cn("overflow-auto", !compact && "max-h-[360px]")}>
        <table className="w-full text-xs">
          <thead className="sticky top-0 z-10 bg-zinc-50">
            <tr className="border-b border-zinc-100">
              {columns.map((col) => (
                <th
                  key={col}
                  className="text-left px-3 py-2 font-medium text-zinc-500 whitespace-nowrap"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {hasRows ? (
              visibleRows.map((row, idx) => (
                <tr key={idx}>
                  {columns.map((col) => (
                    <td
                      key={col}
                      className="px-3 py-2 text-zinc-700 whitespace-nowrap align-top"
                    >
                      {renderCell(col, row[col], taxonomy)}
                    </td>
                  ))}
                </tr>
              ))
            ) : (
              <tr>
                <td
                  colSpan={columns.length}
                  className="px-3 py-4 text-center text-zinc-400"
                >
                  표시할 행이 없습니다
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
