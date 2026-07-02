// 기초분석보고서 패널 렌더러 (공용).
// 데이터셋 버전 "기초분석보고서" 탭(BasicReportTab)과 보고서 에디터의 기초분석 섹션 블록이
// 동일하게 보이도록 layout(rows→panels) 렌더를 여기로 모았다. 계약: basicReport.ts /
// docs/api/report_basic_template.sample.md.
import { BarTrack, DonutChart, DistributionLegend } from "@/components/common/charts";
import type { DonutDatum, LegendDatum } from "@/components/common/charts";
import { cn } from "@/lib/utils";
import {
  Boxes,
  Calendar,
  FileText,
  Settings2,
  User,
  type LucideIcon,
} from "lucide-react";
import type {
  DefinitionListData,
  DistributionData,
  PeriodTimelineData,
  RankData,
  ReportPanel,
  ReportRow,
  StackedData,
  StatGridData,
  StatItem,
  TagListData,
  ValueFormat,
} from "../models/basicReport";

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

// 문서/절 기준 배지 — 카드 헤더 우측.
export function UnitBadge({ unit }: { unit: "doc" | "clause" }) {
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
    case "period_timeline":
      return <PeriodTimelinePanel data={panel.data as PeriodTimelineData} />;
    case "tag_list":
      return <TagListPanel data={panel.data as TagListData} />;
    case "definition_list":
      return <DefinitionListPanel data={panel.data as DefinitionListData} />;
    default:
      return null;
  }
}

// ── 분석 개요(#31) view별 패널 ────────────────────────────────

// YYYY-MM-DD → YYYY.MM.DD. 같은 해면 끝 날짜는 MM.DD로 축약("2025.09.12 – 09.28").
function formatPeriodRange(start: string, end: string): string {
  const dot = (s: string) => s.split("-").join(".");
  if (!start && !end) return "—";
  if (!start) return dot(end);
  if (!end) return dot(start);
  const sameYear = start.slice(0, 4) === end.slice(0, 4);
  return sameYear
    ? `${dot(start)} – ${end.slice(5).split("-").join(".")}`
    : `${dot(start)} – ${dot(end)}`;
}

// 분석 기간 타임라인(#31) — 연도별 기준/비교 연도 · 대상 기간 · 축제 기간.
function PeriodTimelinePanel({ data }: { data: PeriodTimelineData }) {
  const rows = data.rows ?? [];
  if (rows.length === 0) {
    return (
      <div className="text-[12.5px] text-zinc-400">분석 기간이 설정되지 않았습니다.</div>
    );
  }
  return (
    <div className="rounded-2xl border border-zinc-200">
      {rows.map((r, i) => {
        const base = r.role === "base";
        return (
          <div
            key={`${r.year}-${i}`}
            className={cn(
              "grid grid-cols-1 items-center gap-5 px-6 py-6 sm:grid-cols-[120px_1fr_auto]",
              i > 0 && "border-t border-zinc-100",
            )}
          >
            {/* 연도 + 역할 */}
            <div>
              <div
                className={cn(
                  "text-3xl font-extrabold leading-none tracking-tight tabular-nums",
                  base ? "text-violet-600" : "text-zinc-400",
                )}
              >
                {r.year}
              </div>
              <span
                className={cn(
                  "mt-2.5 inline-block rounded-full px-3 py-1 text-[11px] font-bold",
                  base
                    ? "bg-violet-50 text-violet-600"
                    : "bg-zinc-100 text-zinc-500",
                )}
              >
                {r.role_label}
              </span>
            </div>
            {/* 대상 기간 */}
            <div>
              <div className="mb-1.5 text-[12px] font-semibold text-zinc-400">
                대상 기간
              </div>
              <div className="flex flex-wrap items-center gap-2.5 text-lg font-extrabold tracking-tight tabular-nums text-zinc-800">
                <Calendar className="h-[18px] w-[18px] shrink-0 text-zinc-400" />
                {formatPeriodRange(r.target_start, r.target_end)}
                {r.target_days > 0 && (
                  <span className="rounded-full bg-zinc-100 px-2.5 py-1 text-[12px] font-bold text-zinc-500">
                    총 {r.target_days}일
                  </span>
                )}
              </div>
            </div>
            {/* 축제 기간 */}
            <div className="flex items-center gap-3.5 rounded-2xl bg-violet-50 px-5 py-3.5">
              <span className="text-2xl leading-none">🎉</span>
              <div>
                <div className="mb-1.5 text-[12px] font-bold text-violet-600">
                  축제 기간
                </div>
                <div className="inline-block rounded-lg bg-violet-600 px-4 py-1.5 text-base font-extrabold tabular-nums text-white">
                  {formatPeriodRange(r.festival_start, r.festival_end)}
                </div>
              </div>
            </div>
          </div>
        );
      })}
    </div>
  );
}

// 수집 채널/키워드 — chip 목록.
function TagListPanel({ data }: { data: TagListData }) {
  const items = data.items ?? [];
  if (items.length === 0) {
    return <div className="text-[12.5px] text-zinc-400">—</div>;
  }
  return (
    <div className="flex flex-wrap gap-1.5">
      {items.map((it, i) => (
        <span
          key={`${it}-${i}`}
          className="inline-flex rounded-md bg-zinc-100 px-2 py-1 text-[12px] font-medium text-zinc-600"
        >
          {it}
        </span>
      ))}
    </div>
  );
}

// 유형 정의 — 용어 — 설명 목록.
function DefinitionListPanel({ data }: { data: DefinitionListData }) {
  const items = data.items ?? [];
  if (items.length === 0) {
    return <div className="text-[12.5px] text-zinc-400">유형 정의가 없습니다.</div>;
  }
  return (
    <dl className="flex flex-col gap-2">
      {items.map((it, i) => (
        <div
          key={`${it.term}-${i}`}
          className="flex flex-col gap-0.5 rounded-lg border border-zinc-100 bg-zinc-50/60 px-3.5 py-2.5 sm:flex-row sm:gap-3"
        >
          <dt className="shrink-0 text-[12.5px] font-bold text-zinc-700 sm:w-24">
            {it.term}
          </dt>
          <dd className="text-[12.5px] leading-relaxed text-zinc-500">
            {it.description || "—"}
          </dd>
        </div>
      ))}
    </dl>
  );
}

// ── view별 패널 ───────────────────────────────────────────────

// 분석 개요 stat_grid 셀 아이콘 — key별 매핑(#31 디자인 정합). 미지정 key는 아이콘 없음.
const STAT_ICONS: Record<string, LucideIcon> = {
  subject: User,
  model: Boxes,
  unit: FileText,
  steps: Settings2,
};

function StatGridPanel({ data }: { data: StatGridData }) {
  return (
    <div
      className="grid gap-3.5"
      style={{ gridTemplateColumns: "repeat(auto-fit, minmax(190px, 1fr))" }}
    >
      {data.items.map((it: StatItem, i) => {
        const big = it.format === "count" || it.format === "number";
        const main = formatValue(it.value, it.format);
        const sub =
          it.sub === null || it.sub === undefined || it.sub === ""
            ? null
            : String(it.sub);
        const Icon = STAT_ICONS[it.key];
        return (
          <div
            key={`${it.key}-${i}`}
            className="rounded-xl border border-zinc-100 bg-zinc-50/60 px-4 py-3.5"
          >
            {Icon && (
              <div className="mb-3 grid h-9 w-9 place-items-center rounded-lg bg-violet-50 text-violet-600">
                <Icon className="h-[18px] w-[18px]" />
              </div>
            )}
            <div className="text-[12px] font-semibold text-zinc-400">
              {it.label}
            </div>
            <div
              className={cn(
                "mt-2 tracking-tight tabular-nums text-zinc-900",
                big ? "text-3xl font-extrabold leading-none" : "text-sm font-bold",
              )}
            >
              {main}
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
            className="h-5 rounded-md!"
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
const NEUTRAL_KEYS = new Set(["neutral", "중립"]);

function StackedBarPanel({ data }: { data: StackedData }) {
  const { categories } = data;
  const series = data.series.filter((s) => !NEUTRAL_KEYS.has(s.key));
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
      <div className="mt-3.5 flex flex-wrap gap-4.5 border-b border-zinc-100 py-3.5 text-[12.5px] font-semibold text-zinc-500">
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
  return (
    <div className="rounded-xl border border-zinc-100 bg-zinc-50/50 p-4">
      {title && (
        <div className="mb-3.5 text-[13px] font-bold text-zinc-700">{title}</div>
      )}
      <div className="flex flex-col gap-2">
        {items.map((it) => (
          <div key={`${it.rank}-${it.label}`} className="flex items-center gap-2">
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
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

// 기초분석 섹션 본문 — layout(rows → panels)을 12컬럼 그리드로 렌더(카드/제목은 호출부 소유).
export default function BasicReportLayout({
  layout,
  className,
}: {
  layout: ReportRow[];
  className?: string;
}) {
  return (
    <div className={cn("flex flex-col gap-5", className)}>
      {layout.map((row, ri) => (
        <div
          key={ri}
          className="grid grid-cols-1 items-start gap-5 lg:grid-cols-12"
        >
          {row.panels.map((panel, pi) => (
            <PanelCell key={pi} panel={panel} />
          ))}
        </div>
      ))}
    </div>
  );
}
