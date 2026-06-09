import {
  Bar,
  BarChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { ChatChart } from "../models";
import { scaleForChart, unitOf } from "../models";
import DivergingBarView from "./DivergingBarView";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";

const NEUTRAL_COLOR = "#7c3aed";

// ISO datetime (YYYY-MM-DDTHH:MM:SSZ 등)으로 보이면 날짜 부분만 잘라 표시.
const ISO_DATETIME_PREFIX = /^\d{4}-\d{2}-\d{2}T/;
function formatXTick(value: string): string {
  return ISO_DATETIME_PREFIX.test(value) ? value.slice(0, 10) : value;
}

// 증감(delta) 차트 여부 — diverging_bar kind, 또는 (구버전 데이터 호환) point
// 포맷/음수 값. 증감 차트는 0 기준 가로 다이버징 막대(DivergingBarView)로 그린다.
function isDivergingChart(chart: ChatChart): boolean {
  if (chart.kind === "diverging_bar") return true;
  if (chart.kind !== "bar") return false;
  if (chart.yFormat === "point") return true;
  return chart.rows.some((row) => {
    const v = scaleForChart(row[chart.y], chart.yFormat);
    return v !== null && v < 0;
  });
}

export default function ChartView({ chart }: { chart: ChatChart }) {
  const { data: taxonomy } = useTaxonomy();

  // 증감 비교는 전용 다이버징 뷰로 위임.
  if (isDivergingChart(chart)) {
    return <DivergingBarView chart={chart} />;
  }

  const isAspectX = chart.x === ASPECT_FIELD;
  const unit = unitOf(chart.yFormat);
  const data = chart.rows
    .map((row) => {
      const yValue = scaleForChart(row[chart.y], chart.yFormat);
      if (yValue === null) return null;
      const xValue = row[chart.x];
      const rawX = xValue == null ? "—" : String(xValue);
      return { _x: isAspectX ? aspectLabelOf(taxonomy, rawX) : rawX, _y: yValue };
    })
    .filter((d): d is { _x: string; _y: number } => d !== null);

  if (data.length === 0) return null;

  const formatY = (v: number): string => {
    if (chart.yFormat === "percent") return `${v.toFixed(1)}%`;
    return String(v);
  };
  const yName = chart.yLabel ?? chart.y;
  const headerText = [chart.title, unit ? `(단위: ${unit})` : ""].filter(Boolean).join(" ");

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      {headerText && (
        <div className="px-3 py-2 border-b border-zinc-100 text-xs font-medium text-zinc-600">
          {headerText}
        </div>
      )}
      <div className="p-3">
        <ResponsiveContainer width="100%" height={240}>
          {chart.kind === "line" ? (
            <LineChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f4f4f5" />
              <XAxis
                dataKey="_x"
                tick={{ fontSize: 11, fill: "#71717a" }}
                tickFormatter={formatXTick}
                axisLine={false}
                tickLine={false}
              />
              <YAxis tick={{ fontSize: 11, fill: "#a1a1aa" }} axisLine={false} tickLine={false} width={48} />
              <Tooltip
                contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #f4f4f5" }}
                formatter={(v) => [formatY(Number(v)), yName]}
                labelFormatter={(l) => `${chart.x}: ${String(l)}`}
              />
              <Line type="monotone" dataKey="_y" stroke={NEUTRAL_COLOR} strokeWidth={2} dot={{ r: 3, fill: NEUTRAL_COLOR }} />
            </LineChart>
          ) : (
            <BarChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f4f4f5" />
              <XAxis
                dataKey="_x"
                tick={{ fontSize: 11, fill: "#71717a" }}
                tickFormatter={formatXTick}
                axisLine={false}
                tickLine={false}
              />
              <YAxis tick={{ fontSize: 11, fill: "#a1a1aa" }} axisLine={false} tickLine={false} width={48} />
              <Tooltip
                cursor={{ fill: "#f4f4f5" }}
                contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #f4f4f5" }}
                formatter={(v) => [formatY(Number(v)), yName]}
                labelFormatter={(l) => `${chart.x}: ${String(l)}`}
              />
              <Bar dataKey="_y" fill={NEUTRAL_COLOR} radius={[4, 4, 0, 0]} />
            </BarChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  );
}
