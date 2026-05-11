import { Progress } from "@/components/ui/progress";
import type { ProgressType } from "@/features/dataset/types/datasetVersion";
import { cn } from "@/lib/utils";

export function StageProgressBar({ progress }: { progress: ProgressType }) {
  const {message, percent, processedRows, totalRows, elapsedSeconds } = progress
  const isDone= message.includes('completed')
  return (
    <div className="w-full">
      <div className="flex items-center justify-between text-[11px]">
        <span className={cn(isDone ? "text-emerald-500" : "text-amber-500")}>{progress.message}</span>
        <span className={cn("font-mono font-semibold", isDone ? "text-emerald-500" : "text-amber-500")}>
          {percent}%
        </span>
      </div>
      <Progress
        value={percent}
        className={cn("h-1.5", isDone ?"[&>div]:bg-emerald-500": "[&>div]:bg-amber-500")}
      />
      <div className="flex gap-4 mt-1.5 place-self-end text-xs font-mono text-muted-foreground">
        <span>
          {processedRows ?? 0} / {totalRows ?? 0}건
        </span>
        <span>경과 {elapsedSeconds ?? 0}s</span>
      </div>
    </div>
  );
}
