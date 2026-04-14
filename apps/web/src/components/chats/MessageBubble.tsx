import type { ChatMessage } from "@/mock/chatMockData"
import { Avatar, AvatarFallback } from "../ui/avatar"

// ── 메시지 말풍선 ────────────────────────────────────────────────────────────
export default function MessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === 'user'

  function renderContent(text: string) {
    const parts = text.split(/(\*\*[^*]+\*\*)/g)
    return parts.map((part, i) =>
      part.startsWith('**') && part.endsWith('**') ? (
        <strong key={i} className="font-semibold">
          {part.slice(2, -2)}
        </strong>
      ) : (
        <span key={i}>{part}</span>
      )
    )
  }

  return (
    <div className={`flex gap-2.5 ${isUser ? 'flex-row-reverse' : 'flex-row'}`}>
      <Avatar className="h-7 w-7 shrink-0 mt-0.5">
        <AvatarFallback
          className={
            isUser
              ? 'bg-zinc-200 text-zinc-600 text-[10px]'
              : 'bg-violet-100 text-violet-700 text-[10px]'
          }
        >
          {isUser ? 'WN' : 'AI'}
        </AvatarFallback>
      </Avatar>

      <div className={`flex flex-col gap-1 max-w-[75%] ${isUser ? 'items-end' : 'items-start'}`}>
        <div
          className={`rounded-2xl px-3.5 py-2.5 text-[13px] leading-relaxed whitespace-pre-line ${
            isUser
              ? 'bg-zinc-900 text-white rounded-tr-sm'
              : 'bg-white border border-zinc-100 text-zinc-800 rounded-tl-sm'
          }`}
        >
          {renderContent(message.content)}
        </div>
        <span className="text-[10px] text-zinc-400 px-1">{message.timestamp}</span>
      </div>
    </div>
  )
}