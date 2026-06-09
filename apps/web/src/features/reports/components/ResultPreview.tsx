import type { ReportResult, Segment, TablePreview } from "../models/model";

// 결과 카드의 미니 미리보기 — 실제 차트/표 모양을 축소해 보여준다.

function MiniDonut({ segments }: { segments: Segment[] }) {
  const r = 30;
  const c = 2 * Math.PI * r;
  let offset = 0;
  return (
    <svg viewBox="0 0 80 80" className="h-22 w-22">
      <circle cx="40" cy="40" r={r} fill="none" stroke="#f0f0f4" strokeWidth="13" />
      {segments.map((s, i) => {
        const len = (s.value / 100) * c;
        const el = (
          <circle
            key={i}
            cx="40"
            cy="40"
            r={r}
            fill="none"
            stroke={s.color}
            strokeWidth="13"
            strokeDasharray={`${len} ${c - len}`}
            strokeDashoffset={-offset}
            transform="rotate(-90 40 40)"
          />
        );
        offset += len;
        return el;
      })}
    </svg>
  );
}

function MiniBars({ bars }: { bars: { label: string; value: number }[] }) {
  const max = Math.max(...bars.map((b) => b.value), 1);
  return (
    <div className="flex w-full flex-col gap-2 px-1.5 py-1">
      {bars.map((b) => (
        <div
          key={b.label}
          className="grid grid-cols-[46px_1fr] items-center gap-2"
        >
          <span className="truncate text-right text-[10px] font-semibold text-zinc-400">
            {b.label}
          </span>
          <div className="h-1.5 overflow-hidden rounded-full bg-zinc-200/70">
            <div
              className="h-full rounded-full bg-linear-to-r from-blue-500 to-blue-400"
              style={{ width: `${(b.value / max) * 100}%` }}
            />
          </div>
        </div>
      ))}
    </div>
  );
}

function MiniStack({ segments }: { segments: Segment[] }) {
  return (
    <div className="w-full px-2">
      <div className="flex h-5.5 overflow-hidden rounded-md">
        {segments.map((s) => (
          <div key={s.label} style={{ width: `${s.value}%`, background: s.color }} />
        ))}
      </div>
      <div className="mt-2.5 flex justify-center gap-3">
        {segments.map((s) => (
          <span
            key={s.label}
            className="inline-flex items-center gap-1 text-[10px] font-semibold text-zinc-500"
          >
            <i
              className="inline-block h-2 w-2 rounded-xs"
              style={{ background: s.color }}
            />
            {s.label} {s.value}%
          </span>
        ))}
      </div>
    </div>
  );
}

function MiniTable({ table }: { table: TablePreview }) {
  return (
    <div className="h-full w-full text-[10.5px]">
      <div className="grid grid-cols-[1fr_52px_46px] gap-1.5 border-b border-zinc-100 bg-zinc-50 px-3 py-1.5 font-bold text-zinc-400">
        {table.head.map((h, i) => (
          <span key={i} className={i === 0 ? "" : "text-right"}>
            {h}
          </span>
        ))}
      </div>
      {table.rows.map((row, ri) => (
        <div
          key={ri}
          className="grid grid-cols-[1fr_52px_46px] items-center gap-1.5 border-b border-zinc-100 px-3 py-1.5 last:border-b-0"
        >
          {row.map((cell, ci) => (
            <span
              key={ci}
              className={
                ci === 0
                  ? "truncate text-zinc-700"
                  : "text-right tabular-nums text-zinc-500"
              }
            >
              {cell}
            </span>
          ))}
        </div>
      ))}
    </div>
  );
}

export function ResultPreview({ result }: { result: ReportResult }) {
  if (result.viz === "donut" && result.segments)
    return <MiniDonut segments={result.segments} />;
  if (result.viz === "bars" && result.bars)
    return <MiniBars bars={result.bars} />;
  if (result.viz === "stack" && result.segments)
    return <MiniStack segments={result.segments} />;
  if (result.viz === "table" && result.table)
    return <MiniTable table={result.table} />;
  return null;
}
