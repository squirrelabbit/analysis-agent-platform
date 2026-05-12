
import { Clock, CheckCircle2 } from "lucide-react";
import { cn } from "@/lib/utils";
import type { Prompt } from "../../types/prompt";
import { fmtDate } from "../../utils/prompt";

interface Props {
  current: Prompt;
  previous: Prompt;
}

function diffLines(a: string, b: string) {
  const aLines = a.trim().split("\n");
  const bLines = b.trim().split("\n");
  const maxLen = Math.max(aLines.length, bLines.length);

  return Array.from({ length: maxLen }, (_, i) => ({
    old: aLines[i] ?? "",
    next: bLines[i] ?? "",
    changed: (aLines[i] ?? "") !== (bLines[i] ?? ""),
  }));
}

function DiffCol({
  prompt,
  lines,
  side,
  icon,
  iconClass,
}: {
  prompt: Prompt;
  lines: ReturnType<typeof diffLines>;
  side: "old" | "new";
  icon: React.ReactNode;
  iconClass: string;
}) {
  return (
    <div className="flex-1 flex flex-col overflow-hidden min-w-0">
      {/* 컬럼 헤더 */}
      <div className={cn(
        "flex items-center gap-2 px-4 py-2.5 border-b border-border",
        "text-[11px] font-semibold shrink-0 bg-muted/40",
        iconClass
      )}>
        {icon}
        {prompt.version}
        <span className="font-normal text-muted-foreground ml-1">
          {fmtDate(prompt.updatedAt)}
        </span>
      </div>

      {/* 본문 */}
      <div className="flex-1 overflow-y-auto px-4 py-4">
        <pre className="font-mono text-[11px] leading-[1.8]">
          {lines.map((line, i) => {
            const text = side === "old" ? line.old : line.next;
            return (
              <span
                key={i}
                className={cn(
                  "block",
                  line.changed && side === "old" &&
                    "bg-red-50 text-red-500 line-through opacity-60",
                  line.changed && side === "new" &&
                    "bg-emerald-50 text-emerald-600"
                )}
              >
                {text || " "}
              </span>
            );
          })}
        </pre>
      </div>
    </div>
  );
}

export function PromptDiffView({ current, previous }: Props) {
  const lines = diffLines(previous.content, current.content);

  return (
    <div className="flex flex-1 overflow-hidden divide-x divide-border">
      <DiffCol
        prompt={previous}
        lines={lines}
        side="old"
        icon={<Clock className="w-3.5 h-3.5" />}
        iconClass="text-muted-foreground"
      />
      <DiffCol
        prompt={current}
        lines={lines}
        side="new"
        icon={<CheckCircle2 className="w-3.5 h-3.5" />}
        iconClass="text-primary"
      />
    </div>
  );
}