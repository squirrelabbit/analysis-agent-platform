import type { PromptOperation } from "../types/prompt";
import { OPERATION_META } from "../config/prompt";

interface Props {
  operation: PromptOperation;
  count: number;
}

export function PromptGroupHeader({ operation, count }: Props) {
  const { groupLabel } = OPERATION_META[operation];
  return (
    <div className="flex items-center gap-2 px-4 py-2 sticky top-0
                    bg-background z-10">
      <span className="text-[10px] font-mono text-muted-foreground
                       uppercase tracking-[0.8px] whitespace-nowrap">
        {groupLabel}
      </span>
      <div className="flex-1 h-px bg-border" />
      <span className="text-[10px] font-mono text-muted-foreground">
        {count}
      </span>
    </div>
  );
}