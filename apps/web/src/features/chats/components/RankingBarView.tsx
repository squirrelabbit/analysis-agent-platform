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
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";

const BAR_COLOR = "#8b7cf6";
const TRACK_COLOR = "#f1f0fb";
const MAX_CHART_ITEMS = 10;
const SENTIMENT_LABEL: Record<string, string> = {
  positive: "긍정",
  neutral: "중립",
  negative: "부정",
};

// 가로 랭킹 막대 — 집계 결과(count/비율)를 큰 순으로. 트랙 배경 + 우측 값 컬럼.
export default function RankingBarView({ chart }: { chart: ChatChart }) {
  const { data: taxonomy } = useTaxonomy();
  const unit = chart.unit ?? "";

  const labelOfX = (raw: string): string => {
    if (chart.x === ASPECT_FIELD) return aspectLabelOf(taxonomy, raw);
    if (chart.x === "sentiment") return SENTIMENT_LABEL[raw] ?? raw;
    return raw;
  };

  const data = chart.rows
    .map((row) => {
      const yValue = scaleForChart(row[chart.y], chart.yFormat);
      if (yValue === null) return null;
      const xValue = row[chart.x];
      const rawX = xValue == null ? "—" : String(xValue);
      return { _x: labelOfX(rawX), _y: yValue };
    })
    .filter((d): d is { _x: string; _y: number } => d !== null)
    .sort((a, b) => b._y - a._y)
    .slice(0, MAX_CHART_ITEMS);

  if (data.length === 0) return null;

  const fmtVal = (v: number): string => {
    if (chart.yFormat === "percent") return `${v.toFixed(1)}%`;
    return `${Math.round(v).toLocaleString()}${unit}`;
  };

  const labelByX: Record<string, string> = {};
  for (const d of data) labelByX[d._x] = fmtVal(d._y);

  // 최장 막대가 우측 값 컬럼에 닿지 않게 여유.
  const maxVal = data.reduce((m, d) => Math.max(m, d._y), 0);
  const axisMax = maxVal > 0 ? maxVal * 1.08 : 1;

  const subtitle = `상위 ${data.length}개 · 내림차순`;

  const renderValueTick = (props: { x?: number | string; y?: number | string; payload?: { value?: unknown } }) => {
    const text = labelByX[String(props.payload?.value ?? "")];
    if (text === undefined) return <g />;
    return (
      <text
        x={Number(props.x ?? 0)}
        y={Number(props.y ?? 0)}
        dy={4}
        textAnchor="end"
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
        <ResponsiveContainer width="100%" height={Math.max(150, data.length * 40 + 16)}>
          <BarChart layout="vertical" data={data} margin={{ top: 4, right: 12, bottom: 0, left: 8 }} barCategoryGap="30%">
            <XAxis type="number" domain={[0, axisMax]} hide />
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
              width={64}
              tick={renderValueTick}
              axisLine={false}
              tickLine={false}
            />
            <Bar
              yAxisId="left"
              dataKey="_y"
              radius={[5, 5, 5, 5]}
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
