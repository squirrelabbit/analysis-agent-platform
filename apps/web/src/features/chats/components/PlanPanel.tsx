import { useState } from "react";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import type { ChatPlan } from "../models";

// step.display.label/expression이 있으면 그것을 사용해 사람이 읽기 쉬운 형태로
// 표시. label이 없으면 step.id로 fallback. raw params/skill은 노출하지 않는다
// (백엔드가 label/expression으로 의도된 표현을 제공).
export default function PlanPanel({ plan }: { plan: ChatPlan }) {
  const [open, setOpen] = useState(false);
  const stepCount = plan.steps.length;

  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden text-xs">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center gap-1.5 px-3 py-2 text-zinc-600 hover:bg-zinc-50 transition-colors"
      >
        <ChevronRight
          className={cn(
            "w-3.5 h-3.5 transition-transform",
            open && "rotate-90",
          )}
        />
        <span className="font-medium">분석 계획</span>
        <span className="text-zinc-400">({stepCount}단계)</span>
      </button>
      {open && (
        <ol className="border-t border-zinc-100 divide-y divide-zinc-50">
          {plan.steps.map((step, idx) => (
            <li key={step.id} className="px-3 py-2">
              <div className="flex items-baseline gap-2">
                <span className="font-mono text-[10px] text-zinc-400 w-5 shrink-0">
                  {idx + 1}.
                </span>
                <span className="text-[12px] font-medium text-zinc-800 truncate">
                  {step.label || step.id}
                </span>
              </div>
              {step.expression && (
                <div className="mt-1 ml-7 font-mono text-[11px] text-zinc-500 break-all">
                  {step.expression}
                </div>
              )}
            </li>
          ))}
        </ol>
      )}
    </div>
  );
}
