import {
  Bar,
  BarChart,
  Cell,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts";
import type { ChatChart } from "../models";
import { scaleForChart } from "../models";
import { SERIES_BAR as BAR_COLOR, SERIES_TRACK as TRACK_COLOR, SENTIMENT_LABEL } from "../models/theme";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";

const MAX_CHART_ITEMS = 10;

// 가로 랭킹 막대 — 집계 결과(count/비율)를 큰 순으로. 트랙 배경 + 우측 값 컬럼.
export default function RankingBarView({ chart }: { chart: ChatChart }) {
  const { data: taxonomy } = useTaxonomy();
  const unit = chart.unit ?? "";

  const labelOfX = (raw: string): string => {
    if (chart.x === ASPECT_FIELD) return aspectLabelOf(taxonomy, raw);
    if (chart.x === "sentiment") return SENTIMENT_LABEL[raw] ?? raw;
    return raw;
  };

  const toCount = (raw: unknown): number | null => {
    if (typeof raw === "number") return Number.isFinite(raw) ? raw : null;
    if (typeof raw === "string" && raw.trim() !== "") {
      const n = Number(raw);
      return Number.isFinite(n) ? n : null;
    }
    return null;
  };

  const data = chart.rows
    .map((row) => {
      const yValue = scaleForChart(row[chart.y], chart.yFormat);
      if (yValue === null) return null;
      const xValue = row[chart.x];
      const rawX = xValue == null ? "—" : String(xValue);
      const count = chart.countKey ? toCount(row[chart.countKey]) : null;
      return { _x: labelOfX(rawX), _y: yValue, _count: count };
    })
    .filter((d): d is { _x: string; _y: number; _count: number | null } => d !== null)
    .sort((a, b) => b._y - a._y)
    .slice(0, MAX_CHART_ITEMS);

  if (data.length === 0) return null;

  // 비중(%) 막대는 막대 길이=비중, 라벨에 건수를 함께 보인다("X.X% (N건)").
  const fmtVal = (d: { _y: number; _count: number | null }): string => {
    const base =
      chart.yFormat === "percent"
        ? `${d._y.toFixed(1)}%`
        : `${Math.round(d._y).toLocaleString()}${unit}`;
    if (d._count !== null) return `${base} (${d._count.toLocaleString()}건)`;
    return base;
  };

  const labelByX: Record<string, string> = {};
  for (const d of data) labelByX[d._x] = fmtVal(d);

  // 값 컬럼 폭 — "X.X% (N,NNN건)"처럼 건수 라벨이 붙으면 잘리지 않게 넓힌다.
  const longestLabel = Math.max(...Object.values(labelByX).map((t) => t.length), 0);
  const valueColWidth = Math.min(150, Math.max(64, longestLabel * 8 + 16));

  const subtitle = `상위 ${data.length}개 · 내림차순`;

  // 값은 트랙(플롯) 우측 경계 바깥으로 밀어 막대와 분리한다. 최장 막대는 트랙을
  // 꽉 채우고(domain [0,dataMax]), 값은 plot 우측에서 +offset 위치에 좌측정렬.
  const renderValueTick = (props: { x?: number | string; y?: number | string; payload?: { value?: unknown } }) => {
    const text = labelByX[String(props.payload?.value ?? "")];
    if (text === undefined) return <g />;
    return (
      <text
        x={Number(props.x ?? 0) + 10}
        y={Number(props.y ?? 0)}
        dy={4}
        textAnchor="start"
        fontSize={13}
        fontWeight={700}
        fill="#3f3f46"
      >
        {text}
      </text>
    );
  };

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      <div className="px-4 pt-3 pb-1">
        {chart.title && <div className="text-sm font-semibold text-zinc-800">{chart.title}</div>}
        <div className="text-xs text-zinc-400">{subtitle}</div>
      </div>
      <div className="px-2 pb-3">
        <ResponsiveContainer width="100%" height={Math.max(120, data.length * 34 + 16)}>
          <BarChart layout="vertical" data={data} margin={{ top: 4, right: 12, bottom: 0, left: 8 }} barCategoryGap="22%">
            <XAxis type="number" domain={[0, "dataMax"]} hide />
            <YAxis
              yAxisId="left"
              type="category"
              dataKey="_x"
              width={104}
              tick={{ fontSize: 12, fill: "#52525b" }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              type="category"
              dataKey="_x"
              width={valueColWidth}
              tick={renderValueTick}
              axisLine={false}
              tickLine={false}
            />
            <Bar
              yAxisId="left"
              dataKey="_y"
              radius={[5, 5, 5, 5]}
              maxBarSize={22}
              background={{ fill: TRACK_COLOR, radius: 5 }}
            >
              {data.map((_, i) => (
                <Cell key={i} fill={BAR_COLOR} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
