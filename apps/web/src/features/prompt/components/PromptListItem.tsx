import { OperationBadge } from "./OperationBadge";
import { Calendar, GitBranch } from "lucide-react";
import { cn } from "@/lib/utils";
import type { PromptGroup } from "../types/prompt";
import { fmtDate } from "../utils/prompt";

interface Props {
  group: PromptGroup;
  selected: boolean;
  onSelect: () => void;
}

export function PromptListItem({ group, selected, onSelect }: Props) {
  return (
    <div
      onClick={onSelect}
      className={cn(
        "relative px-4 py-3 cursor-pointer transition-colors",
        "border-b border-border",
        "before:absolute before:left-0 before:top-0 before:bottom-0",
        "before:w-0.75 before:rounded-r-sm before:transition-colors",
        selected
          ? "bg-indigo-50 before:bg-indigo-500"
          : "hover:bg-indigo-50/50 before:bg-transparent"
      )}
    >
      {/* 제목 + 뱃지 */}
      <div className="flex items-start gap-2 mb-1.5">
        <span
          className={cn(
            "text-[13px] font-semibold leading-snug flex-1 min-w-0",
            selected ? "text-primary" : "text-foreground"
          )}
        >
          {group.title}
        </span>
        <div className="flex gap-1 shrink-0 mt-0.5">
          <OperationBadge operation={group.operation} />
          <span className="inline-flex items-center px-1.5 py-0.5
                           rounded-full text-[10px] font-mono
                           bg-muted text-muted-foreground border border-border">
            {group.latestVersion}
          </span>
        </div>
      </div>

      {/* 메타 */}
      <div className="flex items-center gap-3 text-[10px] text-muted-foreground mb-1.5">
        <span className="flex items-center gap-1">
          <Calendar className="w-3 h-3" />
          {fmtDate(group.updatedAt)}
        </span>
        <span className="flex items-center gap-1">
          <GitBranch className="w-3 h-3" />
          {group.versions.length}개 버전
        </span>
      </div>

      {/* 요약 */}
      {group.summary && (
        <p className="text-[11px] text-muted-foreground
                      truncate leading-relaxed">
          {group.summary}
        </p>
      )}
    </div>
  );
}