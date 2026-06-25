import { BarTrack, DonutChart, DistributionLegend } from "@/components/common/charts";
import type { DonutDatum, LegendDatum } from "@/components/common/charts";
import { cn } from "@/lib/utils";
import { useBasicAnalysis } from "../../hooks/build.query";
import { BuildTabLoading } from "../BuildStatusMeta";
import type {
  DistributionData,
  RankData,
  ReportBlock,
  ReportPanel,
  StackedData,
  StatGridData,
  StatItem,
  ValueFormat,
} from "../../models/basicReport";

// 기초분석보고서 탭 — 백엔드(useBasicAnalysis) 응답을 기본 템플릿
// (report_basic_template.sample.md) 공통 디자인으로 렌더한다. 데이터 바인딩은 API
// (basicReport.ts 계약) 그대로 유지하고, 패널 렌더만 BasicReportTemplate 디자인에 맞춘다.

// width → 12컬럼 그리드 span (Tailwind JIT가 잡도록 정적 매핑).
const WIDTH_SPAN: Record<string, string> = {
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
  neutral: "#a1a1aa",
  negative: "#ef4444",
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
function formatValue(value: unknown, format?: ValueFormat): string {
  if (value === null || value === undefined || value === "") return "—";
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
      <div className="rounded-2xl border border-zinc-100 bg-white px-5 py-8 text-center text-sm text-zinc-500 shadow-sm">
        표시할 분석 블록이 없습니다.
      </div>
    );
  }

  return (
    <div className="space-y-5">
      {data.missing_sections.length > 0 && (
        <div className="rounded-xl border border-zinc-100 bg-zinc-50 px-4 py-2.5 text-xs text-zinc-500">
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

function BlockCard({ block }: { block: ReportBlock }) {
  const unit = block.unit_basis === "doc" || block.unit_basis === "clause" ? block.unit_basis : null;
  return (
    <div className="rounded-2xl border border-zinc-100 bg-white p-5.5 shadow-sm">
      <div className="mb-4 flex items-start gap-3">
        <div className="text-[15px] font-bold text-zinc-900">{block.title}</div>
        {unit && <UnitBadge unit={unit} />}
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

// rank는 자체 헤더를 그리므로 패널 상단 제목을 중복 출력하지 않는다.
const SELF_TITLED = new Set(["rank"]);

function PanelCell({ panel }: { panel: ReportPanel }) {
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

// 패널 1개 — view에 맞는 렌더러로 분기.
function PanelView({ panel }: { panel: ReportPanel }) {
  switch (panel.view) {
    case "stat_grid":
      return <StatGridPanel data={panel.data as StatGridData} />;
    case "bar":
      return <BarPanel data={panel.data as DistributionData} />;
    case "doughnut":
      return <DoughnutPanel data={panel.data as DistributionData} />;
    case "table":
      return <TablePanel data={panel.data as DistributionData} />;
    case "stacked_bar":
      return <StackedBarPanel data={panel.data as StackedData} />;
    case "rank":
      return <RankPanel data={panel.data as RankData} title={panel.title} />;
    default:
      return null;
  }
}

// ── view별 패널 ───────────────────────────────────────────────

// 분석 개요·문서 개요(stat_grid) — 분리된 강조 카드 그리드.
// format에 따라 톤을 자동 분기: count/number는 큰 숫자 + 보라 강조바(문서 개요),
// text/code는 중간 크기 + 회색 바(분석 개요).
function StatGridPanel({ data }: { data: StatGridData }) {
  return (
    <div
      className="grid gap-3.5"
      style={{ gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))" }}
    >
      {data.items.map((it: StatItem, i) => {
        const big = it.format === "count" || it.format === "number";
        const main = formatValue(it.value, it.format);
        const sub = it.sub === null || it.sub === undefined || it.sub === "" ? null : String(it.sub);
        return (
          <div
            key={`${it.key}-${i}`}
            className="overflow-hidden rounded-xl border border-zinc-100 bg-white p-4.5 shadow-sm"
          >
            <div className="text-[12.5px] font-semibold text-zinc-500">
              {it.label}
            </div>
            <div
              className={cn(
                "mt-2 tracking-tight tabular-nums text-zinc-900",
                big ? "text-3xl font-extrabold leading-none" : "text-[17px] font-bold",
              )}
            >
              {it.format === "code" ? (
                <code className="font-mono text-[15px] text-violet-600">
                  {main}
                </code>
              ) : (
                main
              )}
              {big && it.unit && (
                <span className="ml-1 text-base font-bold text-zinc-400">
                  {it.unit}
                </span>
              )}
            </div>
            {sub && (
              <div className="mt-1.5 text-[11.5px] font-medium text-zinc-400">
                {sub}
              </div>
            )}
          </div>
        );
      })}
    </div>
  );
}

function BarPanel({ data }: { data: DistributionData }) {
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

function DoughnutPanel({ data }: { data: DistributionData }) {
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

function TablePanel({ data }: { data: DistributionData }) {
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

// 긍정·부정 구성비 — 중립(neutral) 제외 후 남은 series를 counts 기준 100%로 재정규화.
const NEUTRAL_KEYS = new Set(["neutral", "neu", "중립"]);

function StackedBarPanel({ data }: { data: StackedData }) {
  const { categories } = data;
  const series = data.series.filter((s) => !NEUTRAL_KEYS.has(s.key));
  // 카테고리별 세그먼트 % (counts로 재정규화). 마지막 세그먼트는 100-누적으로 채워
  // 반올림 오차 없이 막대가 정확히 100%가 되게 한다.
  const pctRows = categories.map((_, ci) => {
    const denom = series.reduce((sum, s) => sum + (s.counts[ci] ?? 0), 0);
    let acc = 0;
    return series.map((s, si) => {
      if (denom <= 0) return 0;
      if (si === series.length - 1) return Math.max(0, 100 - acc);
      const p = Math.round(((s.counts[ci] ?? 0) / denom) * 100);
      acc += p;
      return p;
    });
  });
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
                const p = pctRows[ci][si];
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
