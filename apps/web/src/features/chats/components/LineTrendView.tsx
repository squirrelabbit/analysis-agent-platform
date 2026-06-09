import {
  Area,
  AreaChart,
  CartesianGrid,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { ChatChart } from "../models";
import { scaleForChart } from "../models";

const LINE_COLOR = "#3b82f6";
const EVENT_COLOR = "#d9a05b";

// ISO datetime이면 날짜 부분만. 그 외(월 등)는 그대로.
function dateShort(s: string): string {
  return /^\d{4}-\d{2}-\d{2}T/.test(s) ? s.slice(0, 10) : s;
}
// 축 라벨 — YYYY-MM-DD → M/D, YYYY-MM → M월, 그 외 raw.
function fmtTick(s: string): string {
  let m = /^(\d{4})-(\d{2})-(\d{2})$/.exec(s);
  if (m) return `${Number(m[2])}/${Number(m[3])}`;
  m = /^(\d{4})-(\d{2})$/.exec(s);
  if (m) return `${Number(m[2])}월`;
  return s;
}

// 날짜 추이 라인 — 영역 채움 + 점 + 그리드. 기준일(축제일) 점선 기준선.
export default function LineTrendView({ chart }: { chart: ChatChart }) {
  const data = chart.rows
    .map((row) => {
      const yValue = scaleForChart(row[chart.y], chart.yFormat);
      if (yValue === null) return null;
      const xValue = row[chart.x];
      if (xValue == null) return null;
      return { _x: dateShort(String(xValue)), _y: yValue };
    })
    .filter((d): d is { _x: string; _y: number } => d !== null);

  if (data.length === 0) return null;

  const eventShort = chart.eventDate ? dateShort(chart.eventDate) : undefined;
  const yName = chart.yLabel ?? chart.y;
  const first = data[0]._x;
  const last = data[data.length - 1]._x;
  const subtitle = first === last ? first : `${first} ~ ${last}`;

  const renderXTick = (props: { x?: number | string; y?: number | string; payload?: { value?: unknown } }) => {
    const raw = String(props.payload?.value ?? "");
    const isEvent = !!eventShort && raw === eventShort;
    return (
      <text
        x={Number(props.x ?? 0)}
        y={Number(props.y ?? 0)}
        dy={14}
        textAnchor="middle"
        fontSize={11}
        fontWeight={isEvent ? 700 : 400}
        fill={isEvent ? "#3f3f46" : "#a1a1aa"}
      >
        {fmtTick(raw)}
      </text>
    );
  };

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      <div className="px-4 pt-3 pb-1">
        {chart.title && <div className="text-sm font-semibold text-zinc-800">{chart.title}</div>}
        <div className="text-xs text-zinc-400">{subtitle}</div>
      </div>
      <div className="px-2 pb-2">
        <ResponsiveContainer width="100%" height={260}>
          <AreaChart data={data} margin={{ top: 18, right: 20, bottom: 4, left: 0 }}>
            <defs>
              <linearGradient id="lineTrendFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={LINE_COLOR} stopOpacity={0.18} />
                <stop offset="100%" stopColor={LINE_COLOR} stopOpacity={0.02} />
              </linearGradient>
            </defs>
            <CartesianGrid vertical={false} strokeDasharray="3 3" stroke="#eef0f4" />
            <XAxis
              dataKey="_x"
              tick={renderXTick}
              axisLine={false}
              tickLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tick={{ fontSize: 11, fill: "#a1a1aa" }}
              axisLine={false}
              tickLine={false}
              width={40}
              allowDecimals={false}
            />
            <Tooltip
              contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #f4f4f5" }}
              formatter={(v) => [String(v), yName]}
              labelFormatter={(l) => fmtTick(String(l))}
            />
            {eventShort && (
              <ReferenceLine
                x={eventShort}
                stroke={EVENT_COLOR}
                strokeDasharray="5 3"
                label={{ value: "축제일", position: "top", fill: "#b9772f", fontSize: 11, fontWeight: 600 }}
              />
            )}
            <Area
              type="monotone"
              dataKey="_y"
              stroke={LINE_COLOR}
              strokeWidth={2}
              fill="url(#lineTrendFill)"
              dot={{ r: 4, fill: "#fff", stroke: LINE_COLOR, strokeWidth: 2 }}
              activeDot={{ r: 5 }}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
