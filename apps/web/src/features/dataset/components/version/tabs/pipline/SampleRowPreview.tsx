import { ArrowRight } from "lucide-react";
import { cn } from "@/lib/utils";
export interface SampleRowResult {
  index: number;
  before: string;
  after: string;
  skipped: boolean;
  label?: string;        // sentiment용
  confidence?: number;   // sentiment용
}

function AfterText({ row }: { row: SampleRowResult }) {
  if (row.skipped) {
    return (
      <span className="text-amber-600 font-medium">
        skip: true
      </span>
    );
  }
  // sentiment 결과
  if (row.label != null) {
    const colorMap: Record<string, string> = {
      positive: "text-emerald-600",
      negative: "text-red-500",
      neutral:  "text-muted-foreground",
    };
    return (
      <span className={cn("font-medium", colorMap[row.label])}>
        {row.label}
        {row.confidence != null && (
          <span className="font-mono text-[10px] ml-1 opacity-70">
            ({row.confidence.toFixed(2)})
          </span>
        )}
      </span>
    );
  }
  return <span className="text-emerald-600 font-medium">{row.after}</span>;
}

export function SampleRowPreview({ rows }: { rows: SampleRowResult[] }) {
  return (
    <div className="space-y-1.5">
      {rows.map((row) => (
        <div
          key={row.index}
          className="flex items-start gap-2 px-3 py-2 rounded-md
                     bg-muted/50 text-[11px]"
        >
          <span className="font-mono text-muted-foreground w-5 shrink-0">
            #{row.index}
          </span>
          <span className="flex-1 text-muted-foreground leading-relaxed">
            {row.before}
          </span>
          <ArrowRight className="w-3 h-3 text-muted-foreground
                                  shrink-0 mt-0.5" />
          <span className="flex-1 leading-relaxed">
            <AfterText row={row} />
          </span>
        </div>
      ))}
    </div>
  );
}