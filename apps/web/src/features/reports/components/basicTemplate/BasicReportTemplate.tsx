import { Calendar } from "lucide-react";
import { BarTrack, DonutChart, DistributionLegend } from "@/components/common/charts";
import type { DonutDatum, LegendDatum } from "@/components/common/charts";
import { cn } from "@/lib/utils";
import type {
  BasicBlock,
  BasicPanel,
  BasicReport,
  ColumnsTableData,
  DistData,
  PanelWidth,
  RankData,
  StackedBarData,
  StatGridData,
  StatItem,
  TextData,
  ValueFormat,
} from "../../models/basicTemplate";

// 데이터 기초 분석 보고서 "기본 템플릿" 공통 렌더러.
// 계약(report_basic_template.sample.md)을 따르는 BasicReport를 받아 blocks → layout(행)
// → panels(view)로 렌더한다. 도메인 무지 — data는 패널이 자체 보유한다.

// width → 12컬럼 그리드 span (Tailwind JIT가 잡도록 정적 매핑).
const WIDTH_SPAN: Record<PanelWidth, string> = {
  full: "lg:col-span-12",
  "3/4": "lg:col-span-9",
  "2/3": "lg:col-span-8",
  "1/2": "lg:col-span-6",
  "1/3": "lg:col-span-4",
  "1/4": "lg:col-span-3",
};

// key별 고정 색(감성). 없으면 팔레트 순환.
const KEY_COLOR: Record<string, string> = {
  positive: "#10b981",
  pos: "#10b981",
  neutral: "#a1a1aa",
  neu: "#a1a1aa",
  negative: "#ef4444",
  neg: "#ef4444",
};
const PALETTE = [
  "#7c3aed",
  "#2563eb",
  "#10b981",
  "#f59e0b",
  "#ef4444",
  "#06b6d4",
  "#ec4899",
  "#a1a1aa",
];
const colorFor = (key: string, i: number): string =>
  KEY_COLOR[key] ?? PALETTE[i % PALETTE.length];

// 값 표현 — format 태그에 따른 주 표시 문자열.
function formatValue(value: string | number, format: ValueFormat): string {
  switch (format) {
    case "count":
      return typeof value === "number" ? value.toLocaleString() : String(value);
    case "percent":
      return `${value}%`;
    case "ratio":
      return typeof value === "number" ? value.toFixed(2) : String(value);
    case "number":
    case "code":
    case "text":
    default:
      return String(value);
  }
}

const numFmt = (n: number) => n.toLocaleString();

// ── 카드/공용 조각 ────────────────────────────────────────────

function UnitBadge({ unit }: { unit: "doc" | "clause" }) {
  const isDoc = unit === "doc";
  return (
    <span
      className={cn(
        "ml-auto inline-flex shrink-0 items-center gap-1.5 rounded-full px-2.75 py-1 text-[11px] font-bold",
        isDoc ? "bg-blue-50 text-blue-600" : "bg-violet-50 text-violet-600",
      )}
    >
      <span
        className={cn(
          "h-1.5 w-1.5 rounded-full",
          isDoc ? "bg-blue-600" : "bg-violet-600",
        )}
      />
      {isDoc ? "문서 기준" : "절 기준"}
    </span>
  );
}

// ── view별 패널 ───────────────────────────────────────────────

function StatGridPanel({ data }: { data: StatGridData }) {
  return (
    <div
      className="grid gap-px overflow-hidden rounded-2xl bg-zinc-100"
      style={{ gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))" }}
    >
      {data.items.map((it: StatItem, i) => {
        const main = formatValue(it.value, it.format);
        return (
          <div key={`${it.key}-${i}`} className="bg-white px-4 py-3.5">
            <div className="text-[12px] font-semibold text-zinc-400">
              {it.label}
            </div>
            <div className="mt-1.75 text-[15px] font-bold tracking-tight text-zinc-900">
              {it.format === "code" ? (
                <code className="font-mono text-[13px] text-violet-600">
                  {main}
                </code>
              ) : (
                main
              )}
              {it.format === "count" && it.unit && (
                <span className="ml-1 text-[12.5px] font-semibold text-zinc-400">
                  {it.unit}
                </span>
              )}
              {it.sub && (
                <span className="ml-1.5 text-[12.5px] font-semibold text-zinc-400">
                  · {it.sub}
                </span>
              )}
            </div>
          </div>
        );
      })}
    </div>
  );
}

// 강조 카드 그리드(문서 개요 등) — 큰 숫자 + 범위 뱃지 + 좌측 컬러바.
function StatCardsPanel({ data }: { data: StatGridData }) {
  return (
    <div
      className="grid gap-3.5"
      style={{ gridTemplateColumns: "repeat(auto-fit, minmax(200px, 1fr))" }}
    >
      {data.items.map((it: StatItem, i) => {
        const primary = it.accent === "primary";
        return (
          <div
            key={`${it.key}-${i}`}
            className="relative overflow-hidden rounded-xl border border-zinc-100 bg-white p-4.5 shadow-sm"
          >
            <span
              className={cn(
                "absolute inset-y-0 left-0 w-0.75",
                primary ? "bg-violet-500" : "bg-zinc-300",
              )}
            />
            <div className="flex items-center justify-between gap-2">
              <div className="text-[12.5px] font-semibold text-zinc-600">
                {it.label}
              </div>
              {it.badge && (
                <span
                  className={cn(
                    "inline-flex items-center gap-1.5 rounded-full px-2 py-0.75 text-[11px] font-bold",
                    primary
                      ? "bg-violet-50 text-violet-600"
                      : "bg-zinc-100 text-zinc-500",
                  )}
                >
                  {it.badge}
                </span>
              )}
            </div>
            <div className="my-2 text-3xl font-extrabold leading-none tracking-tight tabular-nums text-zinc-900">
              {formatValue(it.value, it.format)}
              {it.unit && (
                <span className="ml-1 text-base font-bold text-zinc-400">
                  {it.unit}
                </span>
              )}
            </div>
            {it.sub && (
              <div className="inline-flex items-center gap-1.5 text-[11.5px] font-medium text-zinc-400">
                <Calendar className="h-3 w-3" />
                {it.sub}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function BarPanel({ data }: { data: DistData }) {
  return (
    <div className="flex flex-col gap-4">
      {data.items.map((it) => (
        <div
          key={it.key}
          className="grid grid-cols-[110px_1fr_116px] items-center gap-4"
        >
          <span className="truncate text-[13.5px] font-semibold text-zinc-800">
            {it.label}
          </span>
          <BarTrack
            className="h-5 !rounded-md"
            percent={it.percent}
            fillClassName="!rounded-md bg-linear-to-r from-violet-500 to-violet-600 transition-all duration-500"
          />
          <span className="flex items-baseline justify-end gap-2 tabular-nums">
            <span className="text-[15px] font-extrabold text-zinc-900">
              {numFmt(it.count)}
            </span>
            <span className="text-[12.5px] font-bold text-zinc-400">
              {it.percent}%
            </span>
          </span>
        </div>
      ))}
    </div>
  );
}

function DoughnutPanel({ data }: { data: DistData }) {
  const donut: DonutDatum[] = data.items.map((it, i) => ({
    key: it.key,
    value: it.count,
    color: colorFor(it.key, i),
  }));
  const legend: LegendDatum[] = data.items.map((it, i) => ({
    key: it.key,
    label: it.label,
    value: it.count,
    percent: it.percent,
    color: colorFor(it.key, i),
  }));
  const lead = data.items[0];
  return (
    <div className="flex flex-wrap items-center gap-7">
      <DonutChart
        data={donut}
        size={168}
        innerRadius={52}
        outerRadius={74}
        paddingAngle={3}
        center={
          lead ? (
            <div className="text-center">
              <div className="text-2xl font-extrabold leading-none tabular-nums text-zinc-900">
                {lead.percent}%
              </div>
              <div className="mt-1 text-[11px] font-semibold text-zinc-400">
                {lead.label}
              </div>
            </div>
          ) : null
        }
      />
      <DistributionLegend items={legend} className="min-w-55 flex-1" />
    </div>
  );
}

function isColumnsTable(d: DistData | ColumnsTableData): d is ColumnsTableData {
  return "columns" in d;
}

function TablePanel({ data }: { data: DistData | ColumnsTableData }) {
  if (isColumnsTable(data)) {
    // 컬럼이 전부 숫자면 우측, 아니면 좌측 정렬(헤더·셀 동일).
    const alignRight = data.columns.map((_, ci) =>
      data.rows.every((r) => typeof r[ci] === "number"),
    );
    return (
      <div className="overflow-hidden rounded-xl border border-zinc-100">
        <table className="w-full border-collapse text-[12.5px]">
          <thead>
            <tr className="bg-zinc-50/70 text-[11px] font-bold text-zinc-400">
              {data.columns.map((c, i) => (
                <th
                  key={c}
                  className={cn(
                    "px-3 py-2.5",
                    alignRight[i] ? "text-right" : "text-left",
                  )}
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map((row, ri) => (
              <tr key={ri} className="border-t border-zinc-100">
                {row.map((cell, ci) => (
                  <td
                    key={ci}
                    className={cn(
                      "px-3 py-2.25 tabular-nums",
                      alignRight[ci]
                        ? "text-right font-bold text-zinc-900"
                        : "text-left font-semibold text-zinc-800",
                    )}
                  >
                    {typeof cell === "number" ? numFmt(cell) : cell}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    );
  }
  // 분포 표 (label / count / percent + 합계)
  return (
    <div className="overflow-hidden rounded-xl border border-zinc-100">
      <table className="w-full border-collapse text-[12.5px]">
        <thead>
          <tr className="bg-zinc-50/70 text-[11px] font-bold text-zinc-400">
            <th className="px-3 py-2.5 text-left">항목</th>
            <th className="px-3 py-2.5 text-right">수</th>
            <th className="px-3 py-2.5 text-right">비율</th>
          </tr>
        </thead>
        <tbody>
          {data.items.map((it) => (
            <tr key={it.key} className="border-t border-zinc-100">
              <td className="px-3 py-2.25 font-semibold text-zinc-800">
                {it.label}
              </td>
              <td className="px-3 py-2.25 text-right font-bold tabular-nums text-zinc-900">
                {numFmt(it.count)}
              </td>
              <td className="px-3 py-2.25 text-right tabular-nums text-zinc-500">
                {it.percent}%
              </td>
            </tr>
          ))}
        </tbody>
        <tfoot>
          <tr className="border-t border-zinc-200 bg-zinc-50/70 font-extrabold text-zinc-900">
            <td className="px-3 py-2.5">합계</td>
            <td className="px-3 py-2.5 text-right tabular-nums">
              {numFmt(data.total)}
            </td>
            <td className="px-3 py-2.5 text-right tabular-nums">100%</td>
          </tr>
        </tfoot>
      </table>
    </div>
  );
}

function StackedBarPanel({ data }: { data: StackedBarData }) {
  const { categories, series } = data;
  return (
    <div>
      <div className="flex flex-col gap-3">
        {categories.map((cat, ci) => (
          <div
            key={cat.key}
            className="grid grid-cols-[140px_1fr] items-center gap-4"
          >
            <span className="truncate text-[13.5px] font-semibold text-zinc-800">
              {cat.label}
            </span>
            <div className="flex h-6.5 overflow-hidden rounded-md text-[11px] font-extrabold text-white">
              {series.map((s, si) => {
                const p = s.percents[ci] ?? 0;
                return (
                  <span
                    key={s.key}
                    className="grid place-items-center transition-all duration-500"
                    style={{ width: `${p}%`, background: colorFor(s.key, si) }}
                  >
                    {p >= 8 ? `${p}%` : ""}
                  </span>
                );
              })}
            </div>
          </div>
        ))}
      </div>
      <div className="mt-3.5 flex flex-wrap gap-4.5 border-t border-zinc-100 pt-3.5 text-[12.5px] font-semibold text-zinc-500">
        {series.map((s, si) => (
          <span key={s.key} className="inline-flex items-center gap-1.75">
            <i
              className="h-2.75 w-2.75 rounded-sm"
              style={{ background: colorFor(s.key, si) }}
            />
            {s.label}
          </span>
        ))}
      </div>
    </div>
  );
}

function RankPanel({ data, title }: { data: RankData; title?: string }) {
  const items = [...data.items].sort((a, b) => a.rank - b.rank);
  const max = Math.max(...items.map((i) => i.value), 1);
  const total = items.reduce((a, i) => a + i.value, 0);
  return (
    <div className="rounded-xl border border-zinc-100 bg-zinc-50/50 p-4">
      {title && (
        <div className="mb-3.5 flex items-center gap-2 text-[13px] font-bold text-zinc-700">
          {title}
          <span className="ml-auto text-[11.5px] font-semibold text-zinc-400">
            총 {numFmt(total)}
          </span>
        </div>
      )}
      <div className="flex flex-col gap-3">
        {items.map((it) => (
          <div key={`${it.rank}-${it.label}`} className="flex flex-col gap-1.5">
            <div className="flex items-center gap-2">
              <span
                className={cn(
                  "grid h-4.5 w-4.5 shrink-0 place-items-center rounded-md text-[11px] font-extrabold",
                  it.rank === 1
                    ? "bg-violet-500 text-white"
                    : "border border-zinc-200 bg-white text-zinc-500",
                )}
              >
                {it.rank}
              </span>
              <span className="truncate text-[13px] font-semibold text-zinc-800">
                {it.label}
              </span>
              <span className="ml-auto shrink-0 text-[13.5px] font-extrabold tabular-nums text-zinc-900">
                {numFmt(it.value)}
                <span className="ml-1 text-[11px] font-bold text-zinc-400">
                  {total > 0 ? Math.round((it.value / total) * 1000) / 10 : 0}%
                </span>
              </span>
            </div>
            <BarTrack
              className="h-3"
              percent={(it.value / max) * 100}
              fillClassName="bg-violet-500 transition-all duration-500"
            />
          </div>
        ))}
      </div>
    </div>
  );
}

function TextPanel({ data }: { data: TextData }) {
  return (
    <div className="whitespace-pre-wrap text-[13px] leading-relaxed text-zinc-600">
      {data.markdown}
    </div>
  );
}

// 패널 1개 — view에 맞는 렌더러로 분기.
function PanelView({ panel }: { panel: BasicPanel }) {
  switch (panel.view) {
    case "stat_grid":
      return <StatGridPanel data={panel.data as StatGridData} />;
    case "stat_cards":
      return <StatCardsPanel data={panel.data as StatGridData} />;
    case "bar":
      return <BarPanel data={panel.data as DistData} />;
    case "doughnut":
      return <DoughnutPanel data={panel.data as DistData} />;
    case "table":
      return <TablePanel data={panel.data as DistData | ColumnsTableData} />;
    case "stacked_bar":
      return <StackedBarPanel data={panel.data as StackedBarData} />;
    case "rank":
      return <RankPanel data={panel.data as RankData} title={panel.title} />;
    case "text":
      return <TextPanel data={panel.data as TextData} />;
    default:
      return null;
  }
}

// rank/text는 자체 헤더를 그리므로 패널 상단 제목을 중복 출력하지 않는다.
const SELF_TITLED = new Set(["rank", "text"]);

function PanelCell({ panel }: { panel: BasicPanel }) {
  return (
    <div className={cn("min-w-0", WIDTH_SPAN[panel.width] ?? "lg:col-span-12")}>
      {panel.title && !SELF_TITLED.has(panel.view) && (
        <div className="mb-3 text-[12.5px] font-bold text-zinc-600">
          {panel.title}
        </div>
      )}
      <PanelView panel={panel} />
    </div>
  );
}

function BlockCard({ block }: { block: BasicBlock }) {
  return (
    <div
      className={cn(
        !block.bare && "rounded-2xl border border-zinc-100 bg-white p-5.5 shadow-sm",
      )}
    >
      <div className="mb-4 flex items-start gap-3">
        <div className="text-[15px] font-bold text-zinc-900">{block.title}</div>
        {block.unit_basis && <UnitBadge unit={block.unit_basis} />}
      </div>
      <div className="flex flex-col gap-5">
        {block.layout.map((row, ri) => (
          <div key={ri} className="grid grid-cols-1 items-start gap-5 lg:grid-cols-12">
            {row.panels.map((panel, pi) => (
              <PanelCell key={pi} panel={panel} />
            ))}
          </div>
        ))}
      </div>
    </div>
  );
}

// 보고서 전체 — blocks를 흰 카드로 순서대로 렌더.
export default function BasicReportTemplate({
  report,
  className,
}: {
  report: BasicReport;
  className?: string;
}) {
  return (
    <div className={cn("space-y-5", className)}>
      {report.blocks.map((block) => (
        <BlockCard key={block.block_id} block={block} />
      ))}
    </div>
  );
}
