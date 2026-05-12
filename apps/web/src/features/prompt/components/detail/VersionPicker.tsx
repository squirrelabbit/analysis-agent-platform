import { cn } from "@/lib/utils";
import type { Prompt } from "../../types/prompt";

interface Props {
  versions: Prompt[];
  activeVersion: string;
  onChange: (version: string) => void;
  onAddVersion: () => void;
}

export function VersionPicker({
  versions, activeVersion, onChange, onAddVersion,
}: Props) {
  return (
    <div className="flex items-center gap-1.5 flex-wrap">
      {versions.map((v, i) => (
        <button
          key={v.id}
          onClick={() => onChange(v.version)}
          className={cn(
            "inline-flex items-center gap-1.5 px-3 py-1 rounded-full",
            "text-[11px] font-medium border transition-all",
            activeVersion === v.version
              ? "bg-primary text-primary-foreground border-primary"
              : "bg-transparent text-muted-foreground border-border hover:border-foreground/30 hover:text-foreground"
          )}
        >
          {v.version}
          {i === 0 && (
            <span
              className={cn(
                "text-[9px]",
                activeVersion === v.version
                  ? "text-primary-foreground/70"
                  : "text-muted-foreground"
              )}
            >
              최신
            </span>
          )}
        </button>
      ))}

      {/* 새 버전 */}
      <button
        onClick={onAddVersion}
        className="inline-flex items-center gap-1 px-3 py-1 rounded-full
                   text-[11px] text-muted-foreground border border-dashed
                   border-border hover:border-primary hover:text-primary
                   transition-colors"
      >
        <svg className="w-3 h-3" viewBox="0 0 24 24"
             fill="none" stroke="currentColor" strokeWidth={2.5}>
          <line x1="12" y1="5" x2="12" y2="19"/>
          <line x1="5" y1="12" x2="19" y2="12"/>
        </svg>
        새 버전
      </button>
    </div>
  );
}