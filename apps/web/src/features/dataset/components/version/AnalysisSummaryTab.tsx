import { cn } from "@/lib/utils";
import type {
  DatasetVersion,
  PrepareSummary,
} from "../../types/datasetVersion";

function PrepareSummary(props: PrepareSummary) {
  const { inputRowCount, droppedCount, outputRowCount } = props;
  const counts = [
    { label: "원본(input_row_count)", value: inputRowCount.toLocaleString() },
    {
      label: "제거(dropped_count)",
      value: droppedCount.toLocaleString(),
      variant: "danger",
    },
    {
      label: "최종(output_row_count)",
      value: outputRowCount.toLocaleString(),
      variant: "success",
    },
  ];
  return counts.map(({ label, value, variant }) => (
    <div key={label} className="flex-1 text-center px-4 py-3">
      <p className="text-[10px] font-mono text-muted-foreground mb-1 uppercase tracking-wide">
        {label}
      </p>
      <p
        className={cn(
          "text-[18px] font-bold leading-tight",
          variant === "danger" && "text-red-500",
          variant === "success" && "text-emerald-600",
          !variant && "text-foreground",
        )}
      >
        {value}
      </p>
    </div>
  ));
}

// function SentimentSummary() {}

export default function AnalysisSummaryTab(version: DatasetVersion) {
  return (
    <div>
      {version.prepareSummary && (
        <div className="flex border border-border rounded-lg overflow-hidden mb-4 bg-muted/30">
          <PrepareSummary {...version.prepareSummary} />
        </div>
      )}
    </div>
  );
}
