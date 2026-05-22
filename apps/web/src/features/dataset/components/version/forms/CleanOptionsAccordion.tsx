import { useState } from "react"
import { ChevronDown, AlignLeft } from "lucide-react"
import { cn } from "@/lib/utils"

// ── 타입 ──────────────────────────────────────────────────────────────────────
export interface CleanPreprocessOptions {
  removeEnglish:       boolean
  removeNumbers:       boolean
  removeSpecial:       boolean
  removeMonosyllables: boolean
}

interface CleanOptionsAccordionProps {
  value: CleanPreprocessOptions
  onChange: (value: CleanPreprocessOptions) => void
}

// ── 옵션 메타 ─────────────────────────────────────────────────────────────────
const CLEAN_OPTIONS: {
  key: keyof CleanPreprocessOptions
  label: string
  desc: string
}[] = [
  { key: "removeEnglish",       label: "영문 제거",    desc: "영어 단어 및 알파벳 제거" },
  { key: "removeNumbers",       label: "숫자 제거",    desc: "숫자 및 수식 제거" },
  { key: "removeSpecial",       label: "특수문자 제거", desc: "특수기호, 이모지 제거" },
  { key: "removeMonosyllables", label: "단음절 제거",  desc: "1글자 단어 제거" },
]

// ── Toggle Switch ─────────────────────────────────────────────────────────────
function ToggleSwitch({
  checked,
  onChange,
}: {
  checked: boolean
  onChange: (v: boolean) => void
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      onClick={() => onChange(!checked)}
      className={cn(
        "relative w-9 h-5 rounded-full transition-colors shrink-0",
        checked ? "bg-indigo-500" : "bg-zinc-200"
      )}
    >
      <span
        className={cn(
          "absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-all",
          checked ? "left-4.5" : "left-0.5"
        )}
      />
    </button>
  )
}

// ── CleanOptionsAccordion ─────────────────────────────────────────────────────
export function CleanOptionsAccordion({
  value,
  onChange,
}: CleanOptionsAccordionProps) {
  const [open, setOpen] = useState(false)

  const enabledCount = Object.values(value).filter(Boolean).length

  function handleToggle(key: keyof CleanPreprocessOptions, v: boolean) {
    onChange({ ...value, [key]: v })
  }

  return (
    <div className={cn(
      "border rounded-xl overflow-hidden transition-colors",
      open ? "border-indigo-200" : "border-zinc-200"
    )}>
      {/* 헤더 */}
      <button
        type="button"
        onClick={() => setOpen((prev) => !prev)}
        className={cn(
          "w-full flex items-center justify-between px-3.5 py-2.5 transition-colors",
          open
            ? "bg-indigo-50"
            : "bg-zinc-50 hover:bg-zinc-100"
        )}
      >
        <div className="flex items-center gap-2">
          <AlignLeft className={cn(
            "w-3.5 h-3.5 shrink-0",
            open ? "text-indigo-500" : "text-zinc-400"
          )} />
          <span className={cn(
            "text-xs font-medium",
            open ? "text-indigo-700" : "text-zinc-700"
          )}>
            Clean 옵션
          </span>
          {enabledCount > 0 && (
            <span className="text-[10px] px-2 py-0.5 rounded-full bg-indigo-100 text-indigo-600 border border-indigo-200 font-medium">
              {enabledCount}개 설정됨
            </span>
          )}
        </div>
        <ChevronDown className={cn(
          "w-3.5 h-3.5 transition-transform shrink-0",
          open ? "rotate-180 text-indigo-400" : "text-zinc-400"
        )} />
      </button>

      {/* 옵션 목록 */}
      {open && (
        <div className="border-t border-zinc-100 divide-y divide-zinc-100">
          {CLEAN_OPTIONS.map((opt) => (
            <div
              key={opt.key}
              className="flex items-center justify-between px-3.5 py-2.5"
            >
              <div>
                <p className="text-xs font-medium text-zinc-700">{opt.label}</p>
                <p className="text-[10px] text-zinc-400 mt-0.5">{opt.desc}</p>
              </div>
              <ToggleSwitch
                checked={value[opt.key]}
                onChange={(v) => handleToggle(opt.key, v)}
              />
            </div>
          ))}
        </div>
      )}
    </div>
  )
}