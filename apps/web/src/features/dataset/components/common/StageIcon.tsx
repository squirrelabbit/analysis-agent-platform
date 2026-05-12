import { cn } from "@/lib/utils";
import { Loader2 } from "lucide-react";

// type StageStatus = "ready" | "stale" | "not_requested" | "running" | "error"

export default function StageIcon({ status, iconBg }: { status: any; iconBg?: string }) {
// export default function StageIcon({ status, iconBg }: { status: StageStatus; iconBg: string }) {
  if (status === "ready") {
    return (
      <div className={cn("w-8 h-8 rounded-lg flex items-center justify-center shrink-0", iconBg)}>
        <svg className="w-4 h-4 text-green-600" viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5">
          <polyline points="2,8 5.5,11.5 13,3" />
        </svg>
      </div>
    )
  }
  if (status === "running") {
    return (
      <div className={cn("w-8 h-8 rounded-lg flex items-center justify-center shrink-0", iconBg)}>
        <Loader2 className="w-4 h-4 text-indigo-500 animate-spin" />
      </div>
    )
  }
  return (
    <div className={cn("w-8 h-8 rounded-lg flex items-center justify-center shrink-0", iconBg)}>
      <svg className={cn("w-4 h-4", status === "stale" ? "text-amber-500" : "text-zinc-400")} viewBox="0 0 15 15" fill="none" stroke="currentColor" strokeWidth="1.5">
        <circle cx="7.5" cy="7.5" r="5.5" />
        <line x1="7.5" y1="5" x2="7.5" y2="7.5" />
        <circle cx="7.5" cy="10" r=".6" fill="currentColor" />
      </svg>
    </div>
  )
}