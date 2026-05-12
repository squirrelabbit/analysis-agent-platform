import { Input } from "@/components/ui/input";
import { Search } from "lucide-react";
import { OPERATION_META, OPERATION_GROUP_ORDER } from "../config/prompt";
import { cn } from "@/lib/utils";
import type { PromptOperation } from "../types/prompt";

interface Props {
  search: string;
  activeOp: PromptOperation | "all";
  onSearchChange: (v: string) => void;
  onOpChange: (op: PromptOperation | "all") => void;
}

export function PromptFilterBar({
  search,
  activeOp,
  onSearchChange,
  onOpChange,
}: Props) {
  const tabs: Array<{ key: PromptOperation | "all"; label: string }> = [
    { key: "all", label: "전체" },
    ...OPERATION_GROUP_ORDER.map((op) => ({
      key: op,
      label: OPERATION_META[op].label,
    })),
  ];

  return (
    <div className="flex flex-col gap-0">
      {/* 검색 */}
      <div className="px-3 pb-2.5 border-b border-border">
        <div className="relative">
          <Search
            className="absolute left-2.5 top-1/2 -translate-y-1/2
                             w-3.5 h-3.5 "
          />
          <Input
            value={search}
            onChange={(e) => onSearchChange(e.target.value)}
            placeholder="검색…"
            className="pl-7 h-8 text-[12px]"
          />
        </div>
      </div>

      {/* 필터 탭 */}
      <div
        className="flex gap-1.5 px-3 py-2 border-b border-border
                      overflow-x-auto scrollbar-none "
      >
        {tabs.map(({ key, label }) => (
          <button
            key={key}
            onClick={() => onOpChange(key)}
            className={cn(
              "px-2.5 py-1 rounded-full text-[11px] font-medium",
              "border whitespace-nowrap transition-colors shrink-0",
              activeOp === key
                ? "bg-foreground text-background border-foreground"
                : "bg-transparent text-muted-foreground border-border hover:border-foreground/30 hover:text-foreground",
            )}
          >
            {label}
          </button>
        ))}
      </div>
    </div>
  );
}
