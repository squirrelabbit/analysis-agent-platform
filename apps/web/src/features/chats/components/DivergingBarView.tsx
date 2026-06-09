import {
  Bar,
  BarChart,
  Cell,
  ReferenceLine,
  ResponsiveContainer,
  XAxis,
  YAxis,
} from "recharts";
import type { ChatChart } from "../models";
import { scaleForChart } from "../models";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";

// 증가/감소 방향 색 (가치판단 아님 — 증감 표현). 증가=초록, 감소=빨강.
const INCREASE_COLOR = "#3f9e6a";
const DECREASE_COLOR = "#d65a5a";
// 카드로 보여줄 최대 항목 수. 나머지는 상세 데이터에서 본다.
const MAX_CHART_ITEMS = 7;
// 감성 enum → 한글 (차트 축 라벨용).
const SENTIMENT_LABEL: Record<string, string> = {
  positive: "긍정",
  neutral: "중립",
  negative: "부정",
};

// 0 기준 가로 다이버징 막대 — 증가 큰 순(위) → 감소(아래) signed 정렬(백엔드).
// 값은 오른쪽 컬럼에 정렬, 단위는 서브타이틀, 0 점선 + 범례.
export default function DivergingBarView({ chart }: { chart: ChatChart }) {
  const { data: taxonomy } = useTaxonomy();
  const unit = chart.unit ?? "";

  // x(그룹) 값 한글화 — aspect는 taxonomy, sentiment는 긍정/중립/부정, 그 외 raw.
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
    .slice(0, MAX_CHART_ITEMS);

  if (data.length === 0) return null;

  // 값 라벨 — 부호 + 크기 + 단위(건은 정수, %p/%는 소수1). 예: +29.3%p / −30.5%p / +167건.
  const fmtVal = (v: number): string => {
    const mag = unit === "건" ? String(Math.round(Math.abs(v))) : Math.abs(v).toFixed(1);
    if (v === 0) return `0${unit}`;
    const sign = v > 0 ? "+" : "−";
    return `${sign}${mag}${unit}`;
  };
  // 오른쪽 값 컬럼(우 YAxis custom tick)용 — _x별 라벨/색.
  const labelByX: Record<string, { text: string; color: string }> = {};
  for (const d of data) {
    labelByX[d._x] = { text: fmtVal(d._y), color: d._y < 0 ? DECREASE_COLOR : INCREASE_COLOR };
  }

  // 막대 끝이 우측 값 컬럼에 닿지 않게 여유(헤드룸)를 둔다.
  const maxAbs = data.reduce((m, d) => Math.max(m, Math.abs(d._y)), 0);
  const axisBound = maxAbs > 0 ? maxAbs * 1.3 : 1;
  const subtitle = unit ? `단위 ${unit}` : undefined;
  const truncated = chart.rows.length > data.length;

  const renderValueTick = (props: {
    x?: number | string; y?: number | string; payload?: { value?: unknown };
  }) => {
    const info = labelByX[String(props.payload?.value ?? "")];
    if (!info) return <g />;
    return (
      <text
        x={Number(props.x ?? 0)}
        y={Number(props.y ?? 0)}
        dy={4}
        textAnchor="end"
        fontSize={13}
        fontWeight={700}
        fill={info.color}
      >
        {info.text}
      </text>
    );
  };

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      {(chart.title || subtitle) && (
        <div className="px-4 pt-3 pb-1">
          {chart.title && <div className="text-sm font-semibold text-zinc-800">{chart.title}</div>}
          <div className="flex items-center gap-2 text-xs text-zinc-400">
            {subtitle && <span>{subtitle}</span>}
            {truncated && <span>· 상위 {data.length}개 (전체는 상세 데이터)</span>}
          </div>
        </div>
      )}
      <div className="px-2 pb-1">
        <ResponsiveContainer width="100%" height={Math.max(170, data.length * 44 + 24)}>
          <BarChart layout="vertical" data={data} margin={{ top: 6, right: 12, bottom: 0, left: 8 }} barCategoryGap="28%">
            <XAxis type="number" domain={[-axisBound, axisBound]} hide />
            <YAxis
              yAxisId="left"
              type="category"
              dataKey="_x"
              width={96}
              tick={{ fontSize: 12, fill: "#52525b" }}
              axisLine={false}
              tickLine={false}
            />
            <YAxis
              yAxisId="right"
              orientation="right"
              type="category"
              dataKey="_x"
              width={72}
              tick={renderValueTick}
              axisLine={false}
              tickLine={false}
            />
            <ReferenceLine x={0} yAxisId="left" stroke="#d4d4d8" strokeDasharray="4 3" />
            <Bar yAxisId="left" dataKey="_y" radius={[3, 3, 3, 3]}>
              {data.map((d, i) => (
                <Cell key={i} fill={d._y < 0 ? DECREASE_COLOR : INCREASE_COLOR} />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
      <div className="flex items-center justify-center gap-4 pb-3 text-[11px] text-zinc-500">
        <span className="flex items-center gap-1">
          <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ background: DECREASE_COLOR }} />
          감소
        </span>
        <span className="flex items-center gap-1">
          <span className="inline-block h-2.5 w-2.5 rounded-sm" style={{ background: INCREASE_COLOR }} />
          증가
        </span>
      </div>
    </div>
  );
}
