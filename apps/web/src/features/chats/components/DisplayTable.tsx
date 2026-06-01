import type { ChatTableDisplay } from "../models";

const MAX_VISIBLE_ROWS = 100;

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

export default function DisplayTable({ display }: { display: ChatTableDisplay }) {
  const { title, columns, rows } = display;
  const totalRows = rows.length;
  const visibleRows = rows.slice(0, MAX_VISIBLE_ROWS);
  const truncated = totalRows > MAX_VISIBLE_ROWS;
  const hasRows = totalRows > 0;
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
      <div className="overflow-auto max-h-[360px]">
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
                      {formatCell(row[col])}
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
