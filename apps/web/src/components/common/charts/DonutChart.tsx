import { Cell, Pie, PieChart, ResponsiveContainer } from "recharts";
import { cn } from "@/lib/utils";

export type DonutDatum = {
  key: string;
  value: number;
  color: string; // hex
};

/**
 * recharts 도넛 + 선택적 중앙 오버레이.
 * 조각 색은 datum.color로 지정. center에 가운데 표시할 노드를 넘긴다(총합 등).
 */
export function DonutChart({
  data,
  size = 132,
  innerRadius,
  outerRadius,
  paddingAngle = 2,
  center,
  className,
}: {
  data: DonutDatum[];
  size?: number;
  innerRadius?: number;
  outerRadius?: number;
  paddingAngle?: number;
  center?: React.ReactNode;
  className?: string;
}) {
  const outer = outerRadius ?? Math.round(size / 2);
  const inner = innerRadius ?? Math.round(outer * 0.68);
  return (
    <div
      className={cn("relative shrink-0", className)}
      style={{ width: size, height: size }}
    >
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            dataKey="value"
            nameKey="key"
            cx="50%"
            cy="50%"
            innerRadius={inner}
            outerRadius={outer}
            paddingAngle={paddingAngle}
            stroke="none"
          >
            {data.map((d) => (
              <Cell key={d.key} fill={d.color} />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      {center != null && (
        <div className="pointer-events-none absolute inset-0 grid place-items-center">
          {center}
        </div>
      )}
    </div>
  );
}
