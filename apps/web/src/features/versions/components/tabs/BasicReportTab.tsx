import { useMemo } from "react";
import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { useBasicAnalysis } from "../../hooks/build.query";
import { BuildTabLoading } from "../BuildStatusMeta";
import type {
  DistributionData,
  RankData,
  ReportBlock,
  ReportPanel,
  StackedData,
  StatGridData,
  ValueFormat,
} from "../../models/basicReport";

// 감성 키는 고정색, 그 외(유형/채널/키워드)는 보라 단색. stacked_bar 세그먼트·도넛에 공통.
const SENTIMENT_COLOR: Record<string, string> = {
  positive: "#10b981",
  neutral: "#94a3b8",
  negative: "#f43f5e",
};
const NEUTRAL_PALETTE = [
  "#7c3aed",
  "#9333ea",
  "#a855f7",
  "#c084fc",
  "#6366f1",
  "#818cf8",
  "#d8b4fe",
  "#4f46e5",
  "#a78bfa",
];

function colorFor(key: string, index: number): string {
  return SENTIMENT_COLOR[key] ?? NEUTRAL_PALETTE[index % NEUTRAL_PALETTE.length];
}

// width fraction("full"|"2/3"|…) → flex-basis %. 12-magic-number 대신 분수 그대로.
const WIDTH_BASIS: Record<string, string> = {
  full: "100%",
  "3/4": "74%",
  "2/3": "65%",
  "1/2": "48.5%",
  "1/3": "31.5%",
  "1/4": "23%",
};

function fmt(value: unknown, format?: ValueFormat, unit?: string): string {
  if (value === null || value === undefined || value === "") return "—";
  switch (format) {
    case "percent":
      return `${value}%`;
    case "count":
    case "number":
      return `${Number(value).toLocaleString()}${unit ?? ""}`;
    default:
      return `${value}${unit ?? ""}`;
  }
}

export function BasicReportTab() {
  const { data, isLoading, error } = useBasicAnalysis();

  if (isLoading) return <BuildTabLoading />;

  if (error) {
    // 가장 흔한 케이스: clean이 아직 준비 안 됨(400 clean_not_ready).
    const msg =
      (error as { response?: { data?: { detail?: string } } })?.response?.data?.detail ??
      "기초분석보고서를 불러오지 못했습니다.";
    return (
      <div className="rounded-2xl border border-amber-200 bg-amber-50 px-5 py-4 text-sm text-amber-800">
        {msg === "clean_not_ready"
          ? "정제(clean)가 완료되면 기초분석보고서가 표시됩니다."
          : msg}
      </div>
    );
  }

  if (!data || data.blocks.length === 0) {
    return (
      <div className="rounded-2xl border border-slate-200 bg-white px-5 py-8 text-center text-sm text-slate-500">
        표시할 분석 블록이 없습니다.
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {data.missing_sections.length > 0 && (
        <div className="rounded-xl border border-slate-200 bg-slate-50 px-4 py-2.5 text-xs text-slate-500">
          빌드 미완으로 일부 섹션이 제외됨:{" "}
          {data.missing_sections.map((m) => m.section_id).join(", ")}
        </div>
      )}
      {data.blocks.map((block) => (
        <BlockCard key={block.block_id ?? block.section_id} block={block} />
      ))}
    </div>
  );
}

function BlockCard({ block }: { block: ReportBlock }) {
  return (
    <section className="rounded-2xl border border-slate-200 bg-white p-5 shadow-sm">
      <div className="mb-4 flex items-center gap-2">
        <h3 className="text-[15px] font-bold text-slate-900">{block.title}</h3>
        {block.unit_basis && (
          <span className="rounded-md bg-slate-100 px-2 py-0.5 text-[11px] font-semibold text-slate-500">
            {block.unit_basis === "clause" ? "절 단위" : block.unit_basis === "doc" ? "문서 단위" : block.unit_basis}
          </span>
        )}
      </div>
      <div className="space-y-4">
        {block.layout.map((row, ri) => (
          <div key={ri} className="flex flex-wrap items-center gap-4">
            {row.panels.map((panel, pi) => (
              <div
                key={pi}
                className="flex min-w-[220px] flex-1 flex-col justify-center"
                style={{ flexBasis: WIDTH_BASIS[panel.width] ?? "100%" }}
              >
                <Panel panel={panel} />
              </div>
            ))}
          </div>
        ))}
      </div>
    </section>
  );
}

function Panel({ panel }: { panel: ReportPanel }) {
  const title = panel.title ? (
    <div className="mb-2 text-[13px] font-semibold text-slate-600">{panel.title}</div>
  ) : null;

  let body: React.ReactNode = null;
  switch (panel.view) {
    case "stat_grid":
      body = <StatGrid data={panel.data as StatGridData} />;
      break;
    case "bar":
      body = <BarList data={panel.data as DistributionData} format={panel.value_format} />;
      break;
    case "doughnut":
      body = <Doughnut data={panel.data as DistributionData} />;
      break;
    case "table":
      body = <DistTable data={panel.data as DistributionData} />;
      break;
    case "stacked_bar":
      body = <StackedBar data={panel.data as StackedData} />;
      break;
    case "rank":
      body = <RankList data={panel.data as RankData} format={panel.value_format} />;
      break;
    default:
      body = null;
  }

  return (
    <div>
      {title}
      {body}
    </div>
  );
}

function StatGrid({ data }: { data: StatGridData }) {
  return (
    <div className="grid grid-cols-2 gap-3 sm:grid-cols-3 lg:grid-cols-5">
      {data.items.map((it) => (
        <div key={it.key} className="rounded-xl border border-slate-100 bg-slate-50/60 px-3.5 py-3">
          <div className="text-[11px] font-semibold text-slate-400">{it.label}</div>
          <div className="mt-1 text-[15px] font-bold text-slate-900">
            {fmt(it.value, it.format, it.unit)}
          </div>
          {it.sub !== undefined && it.sub !== null && it.sub !== "" && (
            <div className="text-[11px] text-slate-400">{String(it.sub)}</div>
          )}
        </div>
      ))}
    </div>
  );
}

function BarList({ data, format }: { data: DistributionData; format?: ValueFormat }) {
  const max = Math.max(1, ...data.items.map((i) => i.count));
  return (
    <div className="space-y-2">
      {data.items.map((it, idx) => (
        <div key={it.key} className="flex items-center gap-3">
          <div className="w-24 shrink-0 truncate text-[12.5px] text-slate-600" title={it.label}>
            {it.label}
          </div>
          <div className="h-5 flex-1 overflow-hidden rounded bg-slate-100">
            <div
              className="h-full rounded"
              style={{ width: `${(it.count / max) * 100}%`, backgroundColor: colorFor(it.key, idx) }}
            />
          </div>
          <div className="w-24 shrink-0 text-right text-[12px] tabular-nums text-slate-500">
            {format === "percent" ? `${it.percent}%` : it.count.toLocaleString()}
            <span className="text-slate-300"> ({it.percent}%)</span>
          </div>
        </div>
      ))}
    </div>
  );
}

function Doughnut({ data }: { data: DistributionData }) {
  const chart = useMemo(
    () => data.items.map((it, idx) => ({ ...it, color: colorFor(it.key, idx) })),
    [data.items],
  );
  return (
    <div className="flex items-center gap-4">
      <div className="h-40 w-40 shrink-0">
        <ResponsiveContainer width="100%" height="100%">
          <PieChart>
            <Pie
              data={chart}
              dataKey="count"
              nameKey="label"
              innerRadius={42}
              outerRadius={70}
              paddingAngle={1}
            >
              {chart.map((it) => (
                <Cell key={it.key} fill={it.color} />
              ))}
            </Pie>
          </PieChart>
        </ResponsiveContainer>
      </div>
      <ul className="space-y-1.5">
        {chart.map((it) => (
          <li key={it.key} className="flex items-center gap-2 text-[12.5px] text-slate-600">
            <span className="h-2.5 w-2.5 rounded-sm" style={{ backgroundColor: it.color }} />
            <span className="font-medium text-slate-700">{it.label}</span>
            <span className="tabular-nums text-slate-400">
              {it.count.toLocaleString()} ({it.percent}%)
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function DistTable({ data }: { data: DistributionData }) {
  return (
    <table className="w-full text-[12.5px]">
      <thead>
        <tr className="border-b border-slate-200 text-left text-slate-400">
          <th className="py-1.5 font-semibold">구분</th>
          <th className="py-1.5 text-right font-semibold">건수</th>
          <th className="py-1.5 text-right font-semibold">비율</th>
        </tr>
      </thead>
      <tbody>
        {data.items.map((it) => (
          <tr key={it.key} className="border-b border-slate-50">
            <td className="py-1.5 text-slate-700">{it.label}</td>
            <td className="py-1.5 text-right tabular-nums text-slate-600">
              {it.count.toLocaleString()}
            </td>
            <td className="py-1.5 text-right tabular-nums text-slate-500">{it.percent}%</td>
          </tr>
        ))}
        <tr className="font-semibold text-slate-700">
          <td className="py-1.5">합계</td>
          <td className="py-1.5 text-right tabular-nums">{data.total.toLocaleString()}</td>
          <td className="py-1.5 text-right tabular-nums">100%</td>
        </tr>
      </tbody>
    </table>
  );
}

function StackedBar({ data }: { data: StackedData }) {
  return (
    <div className="space-y-2.5">
      <div className="flex flex-wrap gap-3">
        {data.series.map((s, idx) => (
          <span key={s.key} className="flex items-center gap-1.5 text-[11.5px] text-slate-500">
            <span className="h-2.5 w-2.5 rounded-sm" style={{ backgroundColor: colorFor(s.key, idx) }} />
            {s.label}
          </span>
        ))}
      </div>
      {data.categories.map((cat, ci) => (
        <div key={cat.key} className="flex items-center gap-3">
          <div className="w-24 shrink-0 truncate text-[12.5px] text-slate-600" title={cat.label}>
            {cat.label}
          </div>
          <div className="flex h-5 flex-1 overflow-hidden rounded bg-slate-100">
            {data.series.map((s, si) => {
              const pct = s.percents[ci] ?? 0;
              if (pct <= 0) return null;
              return (
                <div
                  key={s.key}
                  className="h-full"
                  style={{ width: `${pct}%`, backgroundColor: colorFor(s.key, si) }}
                  title={`${s.label} ${pct}% (${s.counts[ci] ?? 0})`}
                />
              );
            })}
          </div>
          <div className="w-12 shrink-0 text-right text-[11.5px] tabular-nums text-slate-400">
            {cat.total.toLocaleString()}
          </div>
        </div>
      ))}
    </div>
  );
}

function RankList({ data, format }: { data: RankData; format?: ValueFormat }) {
  return (
    <ol className="space-y-1.5">
      {data.items.map((it) => (
        <li key={`${it.rank}-${it.label}`} className="flex items-center gap-3 text-[13px]">
          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-violet-100 text-[11px] font-bold text-violet-700">
            {it.rank}
          </span>
          <span className="flex-1 truncate text-slate-700">{it.label}</span>
          <span className="tabular-nums text-slate-500">
            {format === "percent" ? `${it.value}%` : it.value.toLocaleString()}
          </span>
        </li>
      ))}
    </ol>
  );
}
