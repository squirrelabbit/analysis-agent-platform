import type { ChatTableDisplay } from "../models";

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return "—";
  if (typeof value === "number") return String(value);
  if (typeof value === "string") return value;
  return JSON.stringify(value);
}

export default function DisplayTable({ display }: { display: ChatTableDisplay }) {
  const { title, columns, rows } = display;
  const hasRows = rows.length > 0;
  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      {title && (
        <div className="px-3 py-2 border-b border-zinc-100 text-xs font-medium text-zinc-600">
          {title}
        </div>
      )}
      <div className="overflow-x-auto">
        <table className="w-full text-xs">
          <thead>
            <tr className="border-b border-zinc-100 bg-zinc-50">
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
              rows.map((row, idx) => (
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
