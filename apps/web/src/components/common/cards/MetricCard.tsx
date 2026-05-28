import { cn } from "@/shared/utils/common";

export function MetricCard({
  label,
  value,
  valueColor,
  sub,
}: {
  label: string;
  value: string | number;
  valueColor?: string;
  sub?: string;
}) {
  return (
    <div className="bg-zinc-50 rounded-xl p-3 border border-zinc-100">
      <div className={cn("text-xl font-semibold tracking-tight", valueColor ?? "text-zinc-900")}>
        {value}
        {sub && (
          <span className="ml-1.5 text-xs font-medium text-zinc-400">{sub}</span>
        )}
      </div>
      <div className="text-xs text-zinc-500 mt-1">{label}</div>
    </div>
  );
}
