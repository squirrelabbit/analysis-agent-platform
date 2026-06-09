import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { ChatChart } from "../models";
import { scaleForChart, unitOf } from "../models";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";

// 증가/감소 방향 색 (가치판단 아님 — 증감 표현 전용). 0 이상=증가, 음수=감소.
// green/red(좋음/나쁨 함의)를 피해 violet/amber 사용.
const INCREASE_COLOR = "#7c3aed";
const DECREASE_COLOR = "#f59e0b";

// ISO datetime (YYYY-MM-DDTHH:MM:SSZ 등)으로 보이면 날짜 부분만 잘라 표시.
// 그 외(임의 문자열/숫자 등)는 그대로 — 도메인 모를 때 임의 가공 금지.
const ISO_DATETIME_PREFIX = /^\d{4}-\d{2}-\d{2}T/;
function formatXTick(value: string): string {
  return ISO_DATETIME_PREFIX.test(value) ? value.slice(0, 10) : value;
}

export default function ChartView({ chart }: { chart: ChatChart }) {
  // x축이 aspect면 한글 label로 변환 (미매칭/미로딩 시 key 유지).
  const { data: taxonomy } = useTaxonomy();
  const isAspectX = chart.x === ASPECT_FIELD;
  const unit = unitOf(chart.yFormat);

  // 백엔드 column_formats(percent/point)면 0~1 비율을 %·%p 스케일(×100)로 올린다.
  // null/문자열은 건너뛴다 — recharts가 깨지지 않게.
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

  // 스케일된 값 표시 — point는 부호+단위, percent는 단위, 그 외 원값.
  const formatY = (v: number): string => {
    if (chart.yFormat === "point") return `${v > 0 ? "+" : ""}${v.toFixed(1)}%p`;
    if (chart.yFormat === "percent") return `${v.toFixed(1)}%`;
    return String(v);
  };
  const yName = chart.yLabel ?? chart.y;
  const headerText = [chart.title, unit ? `(단위: ${unit})` : ""]
    .filter(Boolean)
    .join(" ");

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      {headerText && (
        <div className="px-3 py-2 border-b border-zinc-100 text-xs font-medium text-zinc-600">
          {headerText}
        </div>
      )}
      <div className="p-3">
        <ResponsiveContainer width="100%" height={240}>
          {chart.kind === "bar" ? (
            <BarChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f4f4f5" />
              <XAxis
                dataKey="_x"
                tick={{ fontSize: 11, fill: "#71717a" }}
                tickFormatter={formatXTick}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fontSize: 11, fill: "#a1a1aa" }}
                axisLine={false}
                tickLine={false}
                width={48}
              />
              <Tooltip
                cursor={{ fill: "#f4f4f5" }}
                contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #f4f4f5" }}
                formatter={(v) => [formatY(Number(v)), yName]}
                labelFormatter={(l) => `${chart.x}: ${String(l)}`}
              />
              <Bar dataKey="_y" radius={[4, 4, 0, 0]}>
                {data.map((d, i) => (
                  <Cell
                    key={i}
                    fill={d._y < 0 ? DECREASE_COLOR : INCREASE_COLOR}
                  />
                ))}
              </Bar>
            </BarChart>
          ) : (
            <LineChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f4f4f5" />
              <XAxis
                dataKey="_x"
                tick={{ fontSize: 11, fill: "#71717a" }}
                tickFormatter={formatXTick}
                axisLine={false}
                tickLine={false}
              />
              <YAxis
                tick={{ fontSize: 11, fill: "#a1a1aa" }}
                axisLine={false}
                tickLine={false}
                width={48}
              />
              <Tooltip
                contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #f4f4f5" }}
                formatter={(v) => [formatY(Number(v)), yName]}
                labelFormatter={(l) => `${chart.x}: ${String(l)}`}
              />
              <Line
                type="monotone"
                dataKey="_y"
                stroke="#7c3aed"
                strokeWidth={2}
                dot={{ r: 3, fill: "#7c3aed" }}
              />
            </LineChart>
          )}
        </ResponsiveContainer>
      </div>
    </div>
  );
}
