import { CheckCircle2, AlertTriangle, Clock, Loader2 } from "lucide-react"
import { cn } from "@/lib/utils"
import type { BuildStage } from "@/features/dataset/types/datasetVersion"

// type StageStatus = "ready" | "stale" | "not_requested" | "running" | "error"

interface PipelineSummaryPanelProps {
  buildStages: BuildStage[]
}

const STATUS_CONFIG: Record<string, {
  icon: React.ElementType
  iconClass: string
  nodeClass: string
  label: string
}> = {
  ready:         { icon: CheckCircle2, iconClass: "text-emerald-500", nodeClass: "border-emerald-200",                       label: "완료" },
  stale:         { icon: AlertTriangle, iconClass: "text-amber-400",  nodeClass: "border-amber-200",                         label: "갱신 필요" },
  not_requested: { icon: Clock,         iconClass: "text-zinc-300",   nodeClass: "border-zinc-200",                          label: "미요청" },
  running:       { icon: Loader2,       iconClass: "text-indigo-500 animate-spin", nodeClass: "border-indigo-300 shadow-sm shadow-indigo-100", label: "실행 중" },
  error:         { icon: AlertTriangle, iconClass: "text-red-400",    nodeClass: "border-red-200",                           label: "오류" },
}

// ── 노드 ──────────────────────────────────────────────────────────────────────
function StageNode({ stage }: { stage: BuildStage }) {
  const config = STATUS_CONFIG[stage.status]
  const Icon = config.icon

  return (
    <div className={cn(
      "flex flex-col gap-1 px-3.5 py-2.5 bg-white border rounded-xl min-w-22.5 shrink-0",
      config.nodeClass
    )}>
      <div className="flex items-center gap-1.5">
        <Icon className={cn("w-3.5 h-3.5 shrink-0", config.iconClass)} />
        <span className="text-xs font-semibold text-zinc-800 font-mono">{stage.stage}</span>
      </div>
      <span className="text-[10px] text-zinc-400">{config.label}</span>
    </div>
  )
}

// ── 화살표 ────────────────────────────────────────────────────────────────────
function Arrow() {
  return (
    <div className="w-6 h-px bg-zinc-200 shrink-0 relative after:content-[''] after:absolute after:-right-px after:-top-0.75 after:border-4 after:border-transparent after:border-l-zinc-200" />
  )
}

// ── 병렬 화살표 (분기/수렴) ───────────────────────────────────────────────────
function ForkArrow() {
  return (
    <svg className="w-7 h-10 text-zinc-200 shrink-0" viewBox="0 0 28 40" fill="none">
      <line x1="0" y1="20" x2="14" y2="20" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="14" y1="8"  x2="14" y2="32" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="14" y1="8"  x2="24" y2="8"  stroke="currentColor" strokeWidth="1.2"/>
      <line x1="14" y1="32" x2="24" y2="32" stroke="currentColor" strokeWidth="1.2"/>
      <polyline points="21,5 25,8 21,11"   stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round"/>
      <polyline points="21,29 25,32 21,35" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round"/>
    </svg>
  )
}

function MergeArrow() {
  return (
    <svg className="w-7 h-10 text-zinc-200 shrink-0" viewBox="0 0 28 40" fill="none">
      <line x1="4"  y1="8"  x2="14" y2="8"  stroke="currentColor" strokeWidth="1.2"/>
      <line x1="4"  y1="32" x2="14" y2="32" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="14" y1="8"  x2="14" y2="20" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="14" y1="32" x2="14" y2="20" stroke="currentColor" strokeWidth="1.2"/>
      <line x1="14" y1="20" x2="28" y2="20" stroke="currentColor" strokeWidth="1.2"/>
      <polyline points="24,17 28,20 24,23" stroke="currentColor" strokeWidth="1.2" strokeLinejoin="round"/>
    </svg>
  )
}

const GROUP_ORDER = ["source", "pre_prepare", "prepare", "post_prepare", "post_embedding"]

export function PiplineSummary({ buildStages }: PipelineSummaryPanelProps) {
  const grouped = GROUP_ORDER.reduce<Record<string, BuildStage[]>>((acc, group) => {
    const stages = buildStages.filter(s => s.runGroup === group)
    if (stages.length > 0) acc[group] = stages
    return acc
  }, {})

  const groups = Object.entries(grouped)

  return (
    <div className="flex items-center gap-0 overflow-x-auto py-1">
      {groups.map(([group, stages], idx) => {
        const isParallel = stages.length > 1
        const prevGroup = idx > 0 ? groups[idx - 1] : null
        const prevIsParallel = prevGroup ? prevGroup[1].length > 1 : false

        return (
          <div key={group} className="flex items-center gap-0">
            {/* 화살표 */}
            {idx > 0 && (
              isParallel ? <ForkArrow /> :
              prevIsParallel ? <MergeArrow /> :
              <Arrow />
            )}

            {/* 병렬 */}
            {isParallel ? (
              <div className="flex flex-col gap-2">
                {stages.map(stage => <StageNode key={stage.stage} stage={stage} />)}
              </div>
            ) : (
              <StageNode stage={stages[0]} />
            )}
          </div>
        )
      })}
    </div>
  )
}