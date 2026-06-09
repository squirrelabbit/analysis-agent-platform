import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  LabelList,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { ChatChart } from "../models";
import { scaleForChart } from "../models";
import { useTaxonomy } from "@/features/taxonomy/hooks/taxonomy.query";
import { ASPECT_FIELD, aspectLabelOf } from "@/features/taxonomy/models";

// 증감 방향 색 (가치판단 아님 — 한국 증감 관례: 증가=빨강, 감소=파랑).
const INCREASE_COLOR = "#dc2626";
const DECREASE_COLOR = "#2563eb";
// 차트로 보여줄 최대 항목 수. 나머지는 상세 테이블에서 본다 (silverone 2026-06-09).
const MAX_CHART_ITEMS = 7;

// 0 기준 가로 다이버징 막대 — period_compare_count(건) / period_compare_distribution(%p)
// 의 "변화량"을 0 기준으로 보여준다. 단위/정렬/headline은 백엔드 chart_spec이 결정.
export default function DivergingBarView({ chart }: { chart: ChatChart }) {
  const { data: taxonomy } = useTaxonomy();
  const isAspectX = chart.x === ASPECT_FIELD;
  const unit = chart.unit ?? "";

  // 백엔드가 abs_desc로 정렬해 내려주므로 상위 N개만 차트화(나머지는 상세 테이블).
  const data = chart.rows
    .map((row) => {
      const yValue = scaleForChart(row[chart.y], chart.yFormat);
      if (yValue === null) return null;
      const xValue = row[chart.x];
      const rawX = xValue == null ? "—" : String(xValue);
      return { _x: isAspectX ? aspectLabelOf(taxonomy, rawX) : rawX, _y: yValue };
    })
    .filter((d): d is { _x: string; _y: number } => d !== null)
    .slice(0, MAX_CHART_ITEMS);

  if (data.length === 0) return null;

  // 단위별 값 포맷. 거의 0이면 라벨 생략(과한 0.0 강조 방지).
  const fmt = (v: number): string => {
    const sign = v > 0 ? "+" : "";
    if (unit === "%p") return `${sign}${v.toFixed(1)}%p`;
    if (unit === "%") return `${sign}${v.toFixed(1)}%`;
    if (unit === "건") return `${sign}${Math.round(v)}건`;
    return `${sign}${v}`;
  };
  const isNearZero = (v: number): boolean =>
    unit === "건" ? Math.round(v) === 0 : Math.abs(v) < 0.05;

  const yName = chart.yLabel ?? chart.y;
  const headerText = [chart.title, unit ? `(단위: ${unit})` : ""].filter(Boolean).join(" ");
  const maxAbs = data.reduce((m, d) => Math.max(m, Math.abs(d._y)), 0);
  const axisBound = maxAbs > 0 ? maxAbs * 1.5 : 1;
  const truncated = chart.rows.length > data.length;

  const renderValueLabel = (props: {
    x?: number | string; y?: number | string; width?: number | string; height?: number | string; value?: unknown;
  }) => {
    const num = Number(props.value);
    if (!Number.isFinite(num) || isNearZero(num)) return null;
    const x = Number(props.x ?? 0);
    const y = Number(props.y ?? 0);
    const width = Number(props.width ?? 0);
    const height = Number(props.height ?? 0);
    const cy = y + height / 2;
    // recharts가 음수 막대를 x=0선+음수 width 또는 x=좌측+양수 width로 주므로
    // 막대 실제 좌/우 끝을 min/max로 구해 라벨을 그 바깥에 붙인다.
    const left = Math.min(x, x + width);
    const right = Math.max(x, x + width);
    const positive = num >= 0;
    return (
      <text
        x={positive ? right + 4 : left - 4}
        y={cy}
        fill="#52525b"
        fontSize={11}
        textAnchor={positive ? "start" : "end"}
        dominantBaseline="central"
      >
        {fmt(num)}
      </text>
    );
  };

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden">
      {headerText && (
        <div className="flex items-center justify-between gap-3 px-3 py-2 border-b border-zinc-100 text-xs font-medium text-zinc-600">
          <span>{headerText}</span>
          {truncated && (
            <span className="text-zinc-400 font-normal whitespace-nowrap">
              상위 {data.length}개 · 전체는 상세 데이터
            </span>
          )}
        </div>
      )}
      <div className="p-3">
        <ResponsiveContainer width="100%" height={Math.max(200, data.length * 36 + 48)}>
          <BarChart layout="vertical" data={data} margin={{ top: 8, right: 56, bottom: 0, left: 8 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="#f4f4f5" horizontal={false} />
            <XAxis
              type="number"
              domain={[-axisBound, axisBound]}
              tick={{ fontSize: 11, fill: "#a1a1aa" }}
              axisLine={false}
              tickLine={false}
              tickFormatter={(v) => fmt(Number(v))}
            />
            <YAxis
              type="category"
              dataKey="_x"
              tick={{ fontSize: 11, fill: "#71717a" }}
              axisLine={false}
              tickLine={false}
              width={88}
            />
            <Tooltip
              cursor={{ fill: "#f4f4f5" }}
              contentStyle={{ fontSize: 12, borderRadius: 8, border: "1px solid #f4f4f5" }}
              formatter={(v) => [fmt(Number(v)), yName]}
              labelFormatter={(l) => `${chart.x}: ${String(l)}`}
            />
            <ReferenceLine x={0} stroke="#d4d4d8" />
            <Bar dataKey="_y" radius={[2, 2, 2, 2]}>
              {data.map((d, i) => (
                <Cell key={i} fill={d._y < 0 ? DECREASE_COLOR : INCREASE_COLOR} />
              ))}
              <LabelList dataKey="_y" content={renderValueLabel} />
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </div>
  );
}
