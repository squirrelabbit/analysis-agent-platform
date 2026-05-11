import type { ChatScenario } from "@/mock/chatMockData"
import { ChevronRight } from "lucide-react"

export default function ScenarioCard({
  scenario,
  isActive,
  onClick,
}: {
  scenario: ChatScenario
  isActive: boolean
  onClick: () => void
}) {
  return (
    <button
      onClick={onClick}
      className={`w-full text-left px-3 py-2.5 rounded-lg border transition-all ${
        isActive
          ? 'bg-violet-50 border-violet-300'
          : 'bg-white border-zinc-100 hover:border-violet-200 hover:bg-violet-50/50'
      }`}
    >
      <div className="flex items-center gap-2">
        <span
          className={`w-1.5 h-1.5 rounded-full shrink-0 ${
            isActive ? 'bg-violet-500' : 'bg-zinc-300'
          }`}
        />
        <div className="flex-1 min-w-0">
          <p className={`text-xs font-medium truncate ${isActive ? 'text-violet-700' : 'text-zinc-700'}`}>
            {scenario.name}
          </p>
          <p className="text-[10px] text-zinc-400 truncate">{scenario.description}</p>
        </div>
        <ChevronRight className={`w-3 h-3 shrink-0 ${isActive ? 'text-violet-400' : 'text-zinc-300'}`} />
      </div>
    </button>
  )
}
