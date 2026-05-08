import { Download } from "lucide-react"
import { Button } from "@/components/ui/button"
import type { Artifact } from "@/features/dataset/types/datasetVersion"

// 섹션 카드
export function ResultSection({
  title,
  action,
  children,
}: {
  title: string
  action?: React.ReactNode
  children: React.ReactNode
}) {
  return (
    <div className="bg-white border border-zinc-100 rounded-xl overflow-hidden">
      <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-100">
        <span className="text-xs font-medium text-zinc-800">{title}</span>
        {action}
      </div>
      <div className="p-4">{children}</div>
    </div>
  )
}

// 수치 카드
export function StatCard({
  label,
  value,
  unit,
  sub,
  subColor = "text-zinc-400",
}: {
  label: string
  value: React.ReactNode
  unit?: string
  sub?: string
  subColor?: string
}) {
  return (
    <div className="bg-zinc-50 rounded-lg px-3 py-2.5">
      <p className="text-[10px] text-zinc-400 mb-1">{label}</p>
      <p className="text-base font-semibold text-zinc-800 leading-none">
        {value}
        {unit && <span className="text-xs font-normal text-zinc-400 ml-1">{unit}</span>}
      </p>
      {sub && <p className={`text-[10px] mt-1 ${subColor}`}>{sub}</p>}
    </div>
  )
}

// 바 행
export function BarRow({
  label,
  value,
  total,
  displayValue,
  color = "bg-indigo-400",
}: {
  label: string
  value: number
  total: number
  displayValue: string
  color?: string
}) {
  const pct = total > 0 ? Math.round((value / total) * 100) : 0
  return (
    <div className="flex flex-col gap-1.5">
      <div className="flex items-center justify-between text-xs">
        <span className="text-zinc-500">{label}</span>
        <span className="font-medium text-zinc-700">{displayValue}</span>
      </div>
      <div className="h-1.5 bg-zinc-100 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  )
}

// 다운로드 버튼
export function DownloadButton({
  artifact,
  onDownload,
}: {
  artifact: Artifact
  onDownload: (a: Artifact) => Promise<void>
}) {
  const filename = artifact.uri.split("/").pop() ?? artifact.artifactType

  return (
    <Button
      variant="outline"
      size="sm"
      className="gap-1.5 text-xs h-7 text-zinc-500 hover:text-indigo-600 hover:border-indigo-300 hover:bg-indigo-50"
      onClick={() => onDownload(artifact)}
    >
      <Download className="w-3 h-3" />
      {filename}
    </Button>
  )
}