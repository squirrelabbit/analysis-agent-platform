import { useEffect, useRef, useState } from 'react'
import { Send, ChevronRight, Sparkles, Database } from 'lucide-react'

// shadcn components (npx shadcn@latest add ...)
import { ScrollArea } from '@/components/ui/scroll-area'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Badge } from '@/components/ui/badge'
import { Separator } from '@/components/ui/separator'
import {
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from '@/components/ui/tooltip'
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select'

// mock data
import {
  MOCK_INITIAL_MESSAGES,
  MOCK_PROJECTS,
  MOCK_SCENARIOS,
  MOCK_SUGGESTED_QUESTIONS,
  makeMockReply,
  makeScenarioReply,
  type ChatMessage,
  type ChatScenario,
} from '@/mock/chatMockData'
import ScenarioCard from '@/components/chats/ScenarioCard'
import MessageBubble from '@/components/chats/MessageBubble'

export function ChatPage() {
  const [messages, setMessages] = useState<ChatMessage[]>(MOCK_INITIAL_MESSAGES)
  const [input, setInput] = useState('')
  const [isTyping, setIsTyping] = useState(false)
  const [selectedProjectId, setSelectedProjectId] = useState<string>(MOCK_PROJECTS[0].id)
  const [activeScenarioId, setActiveScenarioId] = useState<string | null>(null)

  const bottomRef = useRef<HTMLDivElement>(null)
  const inputRef = useRef<HTMLInputElement>(null)

  const selectedProject = MOCK_PROJECTS.find((p) => p.id === selectedProjectId)!

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' })
  }, [messages, isTyping])

  function addUserMessage(content: string): ChatMessage {
    const msg: ChatMessage = {
      id: crypto.randomUUID(),
      role: 'user',
      content,
      timestamp: new Date().toLocaleTimeString('ko-KR', { hour: '2-digit', minute: '2-digit' }),
    }
    setMessages((prev) => [...prev, msg])
    return msg
  }

  function simulateReply(replyFn: () => ChatMessage) {
    setIsTyping(true)
    setTimeout(() => {
      setIsTyping(false)
      setMessages((prev) => [...prev, replyFn()])
    }, 800)
  }

  function handleSend() {
    const text = input.trim()
    if (!text || isTyping) return
    addUserMessage(text)
    setInput('')
    simulateReply(() => makeMockReply(text))
  }

  function handleSuggestedQuestion(label: string) {
    addUserMessage(label)
    simulateReply(() => makeMockReply(label))
    inputRef.current?.focus()
  }

  function handleScenarioClick(scenario: ChatScenario) {
    if (activeScenarioId === scenario.id) return
    setActiveScenarioId(scenario.id)
    addUserMessage(`${scenario.name} 시나리오로 분석을 시작해줘`)
    simulateReply(() => makeScenarioReply(scenario.name, selectedProject.name))
  }

  return (
    <TooltipProvider delayDuration={300}>
      <div className="flex h-full overflow-hidden bg-zinc-50">

        {/* ── 채팅 영역 ── */}
        <div className="flex flex-col flex-1 overflow-hidden">

          {/* 헤더 */}
          <div className="flex items-center justify-between px-5 py-3 bg-white border-b border-zinc-100 shrink-0">
            <div>
              <h2 className="text-sm font-medium text-zinc-900">통합 분석 채팅</h2>
              <p className="text-[11px] text-zinc-400 mt-0.5">프로젝트별 채팅 분리는 추후 지원 예정</p>
            </div>
            <div className="flex items-center gap-2">
              <span className="text-[11px] text-zinc-500">연결 프로젝트</span>
              <Select value={selectedProjectId} onValueChange={setSelectedProjectId}>
                <SelectTrigger className="w-44 h-7 text-[11px]">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {MOCK_PROJECTS.map((p) => (
                    <SelectItem key={p.id} value={p.id}>{p.name}</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>

          {/* 메시지 목록 */}
          <ScrollArea className="flex-1 px-5 py-4">
            <div className="flex flex-col gap-4 max-w-3xl mx-auto">
              {messages.map((msg) => (
                <MessageBubble key={msg.id} message={msg} />
              ))}

              {/* 타이핑 인디케이터 */}
              {isTyping && (
                <div className="flex gap-2.5 items-start">
                  <Avatar className="h-7 w-7 shrink-0 mt-0.5">
                    <AvatarFallback className="bg-violet-100 text-violet-700 text-[10px]">AI</AvatarFallback>
                  </Avatar>
                  <div className="bg-white border border-zinc-100 rounded-2xl rounded-tl-sm px-4 py-3">
                    <div className="flex gap-1 items-center h-3">
                      {[0, 1, 2].map((i) => (
                        <span
                          key={i}
                          className="w-1.5 h-1.5 rounded-full bg-zinc-400 animate-bounce"
                          style={{ animationDelay: `${i * 0.15}s` }}
                        />
                      ))}
                    </div>
                  </div>
                </div>
              )}

              <div ref={bottomRef} />
            </div>
          </ScrollArea>

          {/* 예상 질문 + 입력창 */}
          <div className="border-t border-zinc-100 bg-white px-5 py-3 shrink-0">
            <div className="flex gap-2 flex-wrap mb-2.5">
              {MOCK_SUGGESTED_QUESTIONS.map((q) => (
                <Tooltip key={q.id}>
                  <TooltipTrigger asChild>
                    <button
                      onClick={() => handleSuggestedQuestion(q.label)}
                      disabled={isTyping}
                      className="text-[11px] px-2.5 py-1 rounded-full border border-zinc-200 bg-zinc-50 text-zinc-500 hover:border-violet-300 hover:bg-violet-50 hover:text-violet-600 transition-colors disabled:opacity-40"
                    >
                      {q.label}
                    </button>
                  </TooltipTrigger>
                  <TooltipContent>클릭하면 질문이 전송됩니다</TooltipContent>
                </Tooltip>
              ))}
            </div>

            <div className="flex gap-2">
              <Input
                ref={inputRef}
                value={input}
                onChange={(e) => setInput(e.target.value)}
                onKeyDown={(e) => e.key === 'Enter' && !e.shiftKey && handleSend()}
                placeholder="분석 질문을 입력하세요..."
                className="flex-1 h-9 text-sm"
                disabled={isTyping}
              />
              <Button
                variant="secondary"
                size="sm"
                onClick={handleSend}
                disabled={!input.trim() || isTyping}
                className="px-4 h-9"
              >
                <Send className="w-3.5 h-3.5" />
              </Button>
            </div>
          </div>
        </div>

        {/* ── 오른쪽 패널 ── */}
        <div className="w-64 shrink-0 border-l border-zinc-100 bg-white flex flex-col overflow-hidden">

          {/* 시나리오 섹션 */}
          <div className="flex-1 overflow-hidden flex flex-col">
            <div className="flex items-center gap-1.5 px-4 py-3 border-b border-zinc-100 shrink-0">
              <Sparkles className="w-3.5 h-3.5 text-violet-500" />
              <span className="text-xs font-medium text-zinc-800">분석 시나리오</span>
            </div>
            <ScrollArea className="flex-1">
              <div className="p-3 flex flex-col gap-1.5">
                {MOCK_SCENARIOS.map((sc) => (
                  <ScenarioCard
                    key={sc.id}
                    scenario={sc}
                    isActive={activeScenarioId === sc.id}
                    onClick={() => handleScenarioClick(sc)}
                  />
                ))}
              </div>
            </ScrollArea>
          </div>

          <Separator />

          {/* 데이터셋 섹션 */}
          <div className="shrink-0">
            <div className="flex items-center gap-1.5 px-4 py-3 border-b border-zinc-100">
              <Database className="w-3.5 h-3.5 text-zinc-400" />
              <span className="text-xs font-medium text-zinc-800">프로젝트 데이터셋</span>
            </div>
            <div className="p-3 flex flex-col gap-1.5">
              {['sales_q4.csv', 'channel_map.xlsx', 'customer.json'].map((name) => (
                <div
                  key={name}
                  className="flex items-center gap-2 px-2.5 py-1.5 rounded-md bg-zinc-50 border border-zinc-100 hover:border-violet-200 hover:bg-violet-50 transition-colors cursor-pointer"
                >
                  <span className="w-1.5 h-1.5 rounded-full bg-zinc-300 shrink-0" />
                  <span className="text-[11px] text-zinc-600 truncate">{name}</span>
                </div>
              ))}
              <Badge variant="default" className="w-fit mt-1 text-[10px]">
                {selectedProject.name}
              </Badge>
            </div>
          </div>
        </div>

      </div>
    </TooltipProvider>
  )
}