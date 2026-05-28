import { cn } from "@/shared/utils/common";

export function MetricCard({
  label,
  value,
  valueColor,
}: {
  label: string;
  value: string | number;
  valueColor?: string;
}) {
  return (
    <div className="bg-zinc-50 rounded-xl p-3 border border-zinc-100">
      <div className={cn("text-xl font-semibold tracking-tight", valueColor ?? "text-zinc-900")}>
        {value}
      </div>
      <div className="text-xs text-zinc-500 mt-1">{label}</div>
    </div>
  );
}
