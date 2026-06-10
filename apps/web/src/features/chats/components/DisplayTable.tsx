import { cn } from "@/lib/utils";
import type { ChatTableDisplay, ColumnFormat } from "../models";
import { formatCellValue } from "../models";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";
import type { Taxonomy } from "@/features/taxonomy/models";

const MAX_VISIBLE_ROWS = 100;
// 20행 이하면 max-height 없이 자연 펼침, 그 이상은 360px 박스 + 내부 스크롤.
const COMPACT_ROWS_THRESHOLD = 20;

// aspect 컬럼의 영문 key 셀은 taxonomy 한글 label로 표시 (미매칭/미로딩 시 key 유지).
// 그 외에는 백엔드 column_formats(%/%p/정수) 기준으로 렌더, 없으면 raw.
function renderCell(
  col: string,
  value: unknown,
  taxonomy: Taxonomy | undefined,
  format: ColumnFormat | undefined,
): string {
  if (col === ASPECT_FIELD && typeof value === "string") {
    return aspectLabelOf(taxonomy, value);
  }
  return formatCellValue(value, format);
}

// 숫자 셀 판정 (number 또는 숫자 문자열).
function isNumericValue(v: unknown): boolean {
  if (typeof v === "number") return Number.isFinite(v);
  if (typeof v === "string") return v.trim() !== "" && Number.isFinite(Number(v));
  return false;
}

export default function DisplayTable({ display }: { display: ChatTableDisplay }) {
  const { title, columns, rows, columnFormats, columnLabels } = display;
  // 조회 실패해도 renderCell이 key로 fallback하므로 화면은 동작한다.
  const { data: taxonomy } = useTaxonomy();
  const totalRows = rows.length;
  const visibleRows = rows.slice(0, MAX_VISIBLE_ROWS);
  const truncated = totalRows > MAX_VISIBLE_ROWS;
  const hasRows = totalRows > 0;
  const compact = totalRows <= COMPACT_ROWS_THRESHOLD;

  // 합계 행 — 숫자 컬럼(모든 값이 숫자/널)을 전체 rows 기준으로 합산. 2행 이상 +
  // 숫자 컬럼이 1개 이상일 때만 표시. 합은 표시된 100행이 아니라 전체 rows 기준.
  // 첫 컬럼(보통 범주)은 "합계" 라벨, 그 외 숫자 컬럼은 같은 column_format으로 렌더.
  const numericCols = columns.filter(
    (col) =>
      col !== columns[0] &&
      rows.some((r) => isNumericValue(r[col])) &&
      rows.every((r) => r[col] == null || isNumericValue(r[col])),
  );
  const showTotals = hasRows && totalRows >= 2 && numericCols.length > 0;
  const totals: Record<string, number> = {};
  if (showTotals) {
    for (const col of numericCols) {
      totals[col] = rows.reduce(
        (acc, r) => acc + (isNumericValue(r[col]) ? Number(r[col]) : 0),
        0,
      );
    }
  }
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
                  {columnLabels?.[col] ?? col}
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
                      {renderCell(col, row[col], taxonomy, columnFormats?.[col])}
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
          {showTotals && (
            <tfoot className="sticky bottom-0 z-10 bg-zinc-50 border-t border-zinc-200">
              <tr className="font-medium text-zinc-700">
                {columns.map((col, i) => (
                  <td key={col} className="px-3 py-2 whitespace-nowrap">
                    {i === 0
                      ? "합계"
                      : col in totals
                        ? formatCellValue(totals[col], columnFormats?.[col])
                        : ""}
                  </td>
                ))}
              </tr>
            </tfoot>
          )}
        </table>
      </div>
    </div>
  );
}
