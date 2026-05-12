import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { OPERATION_META } from "../config/prompt";
import type { PromptOperation } from "../types/prompt";

interface Props {
  operation: PromptOperation;
  className?: string;
}

export function OperationBadge({ operation, className }: Props) {
  const meta = OPERATION_META[operation];
  return (
    <Badge
      variant="outline"
      className={cn(
        "text-[10px] font-mono font-semibold px-2 py-0.5",
        meta.badgeClass,
        className
      )}
    >
      {meta.label}
    </Badge>
  );
}

export function StatusBadge({ status }: { status: string }) {
  const styleMap: Record<string, string> = {
    active:     "bg-emerald-50 text-emerald-700 border-emerald-200",
    ready:      "bg-emerald-50 text-emerald-700 border-emerald-200",
    deprecated: "bg-muted text-muted-foreground border-border",
  };
  return (
    <Badge
      variant="outline"
      className={cn(
        "text-[10px] font-semibold",
        styleMap[status] ?? styleMap.deprecated
      )}
    >
      {status}
    </Badge>
  );
}