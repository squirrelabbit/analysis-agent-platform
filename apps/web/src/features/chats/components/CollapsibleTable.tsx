import { useState } from "react";
import { ChevronRight } from "lucide-react";
import { cn } from "@/lib/utils";
import type { ChatTableDisplay } from "../models";
import DisplayTable from "./DisplayTable";

// 메인 결과로 chart가 표시될 때 상세 데이터 표를 접이식으로 보여주는 래퍼.
export default function CollapsibleTable({
  display,
}: {
  display: ChatTableDisplay;
}) {
  const [open, setOpen] = useState(false);
  return (
    <div className="mt-2 rounded-lg border border-zinc-200 bg-white overflow-hidden text-xs">
      <button
        type="button"
        onClick={() => setOpen((v) => !v)}
        className="w-full flex items-center justify-between px-3 py-2 text-zinc-600 hover:bg-zinc-50 transition-colors"
      >
        <span className="flex items-center gap-1.5">
          <ChevronRight
            className={cn(
              "w-3.5 h-3.5 transition-transform",
              open && "rotate-90",
            )}
          />
          <span className="font-medium">상세 데이터 보기</span>
          <span className="text-zinc-400">
            ({display.rows.length.toLocaleString()}행)
          </span>
        </span>
      </button>
      {open && (
        <div className="border-t border-zinc-100">
          <DisplayTable display={display} />
        </div>
      )}
    </div>
  );
}
