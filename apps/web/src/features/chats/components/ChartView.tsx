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

function coerceNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return value;
  if (typeof value === "string") {
    const n = Number(value);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

export default function ChartView({ chart }: { chart: ChatChart }) {
  // numeric만 통과시키고 null/문자열은 건너뛴다 — recharts가 깨지지 않게.
  const data = chart.rows
    .map((row) => {
      const yValue = coerceNumber(row[chart.y]);
      if (yValue === null) return null;
      const xValue = row[chart.x];
      return { _x: xValue == null ? "—" : String(xValue), _y: yValue };
    })
    .filter((d): d is { _x: string; _y: number } => d !== null);

  if (data.length === 0) return null;

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      {chart.title && (
        <div className="px-3 py-2 border-b border-zinc-100 text-xs font-medium text-zinc-600">
          {chart.title}
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
                formatter={(v) => [String(v), chart.y]}
                labelFormatter={(l) => `${chart.x}: ${String(l)}`}
              />
              <Bar dataKey="_y" fill="#7c3aed" radius={[4, 4, 0, 0]} />
            </BarChart>
          ) : (
            <LineChart data={data} margin={{ top: 8, right: 16, bottom: 0, left: 0 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f4f4f5" />
              <XAxis
                dataKey="_x"
                tick={{ fontSize: 11, fill: "#71717a" }}
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
                formatter={(v) => [String(v), chart.y]}
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
