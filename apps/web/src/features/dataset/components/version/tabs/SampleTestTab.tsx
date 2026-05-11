import { useState } from "react"
import { Play, Loader2, RotateCcw, AlertCircle } from "lucide-react"
import { Button } from "@/components/ui/button"
import { Textarea } from "@/components/ui/textarea"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { cn } from "@/lib/utils"

// ── 타입 ──────────────────────────────────────────────────────────────────────
interface SampleResult {
  input: string
  output: string
  rowCount: number
  elapsed: string
}

interface SampleTestTabProps {
  versionId: string
}

// ── Mock 결과 ─────────────────────────────────────────────────────────────────
const MOCK_RESULT: SampleResult = {
  input: "강릉 커피거리 진짜 너무 좋았어요!! 날씨도 완벽 ㅠㅠ",
  output: `[
  {
    "original": "강릉 커피거리 진짜 너무 좋았어요!! 날씨도 완벽 ㅠㅠ",
    "cleaned": "강릉 커피거리 너무 좋았어요 날씨도 완벽",
    "tokens": ["강릉", "커피거리", "너무", "좋았어요", "날씨도", "완벽"],
    "length": 18
  }
]`,
  rowCount: 10,
  elapsed: "1.2s",
}

// ── SampleTestTab ─────────────────────────────────────────────────────────────
export function SampleTestTab({ versionId: _versionId }: SampleTestTabProps) {
  const [sampleCount, setSampleCount] = useState(10)
  const [manualText, setManualText] = useState("")
  const [inputMode, setInputMode] = useState<"auto" | "manual">("auto")
  const [isRunning, setIsRunning] = useState(false)
  const [result, setResult] = useState<SampleResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  async function handleRun() {
    setIsRunning(true)
    setError(null)
    setResult(null)
    try {
      // TODO: API 연동
      // const res = await datasetApi.runSample(versionId, {
      //   mode: inputMode,
      //   count: sampleCount,
      //   text: inputMode === "manual" ? manualText : undefined,
      // })
      await new Promise((r) => setTimeout(r, 1400))
      setResult(MOCK_RESULT)
    } catch {
      setError("샘플 실행 중 오류가 발생했습니다. 다시 시도해주세요.")
    } finally {
      setIsRunning(false)
    }
  }

  function handleReset() {
    setResult(null)
    setError(null)
    setManualText("")
  }

  return (
    <div className="flex flex-col gap-4">

      {/* 안내 배너 */}
      <div className="flex items-start gap-2.5 px-3.5 py-3 bg-amber-50 border border-amber-200 rounded-xl">
        <AlertCircle className="w-4 h-4 text-amber-500 shrink-0 mt-0.5" />
        <div>
          <p className="text-xs font-medium text-amber-700">LLM 비용 절감용 사전 검증</p>
          <p className="text-xs text-amber-600 mt-0.5 leading-relaxed">
            전체 분석 실행 전 소량 샘플로 분석 결과를 먼저 확인하세요.
            문제가 없으면 파이프라인 탭에서 분석을 실행하세요.
          </p>
        </div>
      </div>

      {/* 입력 모드 토글 */}
      <div className="flex flex-col gap-3 bg-white border border-zinc-100 rounded-xl p-4">
        <div className="flex gap-2">
          {(["auto", "manual"] as const).map((mode) => (
            <button
              key={mode}
              onClick={() => { setInputMode(mode); setResult(null) }}
              className={cn(
                "flex-1 py-2 rounded-lg border text-xs font-medium transition-colors",
                inputMode === mode
                  ? "bg-indigo-500 text-white border-indigo-500"
                  : "bg-white text-zinc-600 border-zinc-200 hover:border-zinc-400"
              )}
            >
              {mode === "auto" ? "파일에서 추출" : "직접 입력"}
            </button>
          ))}
        </div>

        {/* 파일에서 추출 */}
        {inputMode === "auto" && (
          <div className="flex items-center gap-3">
            <Label className="text-xs text-zinc-600 shrink-0">추출 건수</Label>
            <Input
              type="number"
              min={1}
              max={100}
              value={sampleCount}
              onChange={(e) => setSampleCount(Number(e.target.value))}
              className="w-24 h-8 text-xs"
            />
            <p className="text-xs text-zinc-400">건 (최대 100건)</p>
          </div>
        )}

        {/* 직접 입력 */}
        {inputMode === "manual" && (
          <div className="flex flex-col gap-1.5">
            <Label className="text-xs text-zinc-600">텍스트 직접 입력</Label>
            <Textarea
              value={manualText}
              onChange={(e) => setManualText(e.target.value)}
              placeholder="전처리할 텍스트를 입력하세요&#10;여러 줄 입력 시 줄별로 처리됩니다"
              className="text-xs min-h-20 resize-none"
            />
          </div>
        )}

        {/* 실행 버튼 */}
        <div className="flex gap-2 pt-1">
          <Button
            onClick={handleRun}
            disabled={isRunning || (inputMode === "manual" && !manualText.trim())}
            className="gap-1.5 bg-indigo-500 hover:bg-indigo-600 text-white flex-1"
            size="sm"
          >
            {isRunning ? (
              <><Loader2 className="w-3.5 h-3.5 animate-spin" />실행 중...</>
            ) : (
              <><Play className="w-3.5 h-3.5" />샘플 실행</>
            )}
          </Button>
          {result && (
            <Button variant="outline" size="sm" onClick={handleReset}>
              <RotateCcw className="w-3.5 h-3.5" />
            </Button>
          )}
        </div>
      </div>

      {/* 에러 */}
      {error && (
        <div className="flex items-center gap-2 px-3.5 py-3 bg-red-50 border border-red-200 rounded-xl">
          <AlertCircle className="w-4 h-4 text-red-500 shrink-0" />
          <p className="text-xs text-red-600">{error}</p>
        </div>
      )}

      {/* 결과 */}
      {result && (
        <div className="flex flex-col gap-3 bg-white border border-zinc-100 rounded-xl overflow-hidden">
          {/* 결과 헤더 */}
          <div className="flex items-center justify-between px-4 py-3 border-b border-zinc-100">
            <p className="text-xs font-medium text-zinc-700">실행 결과</p>
            <div className="flex items-center gap-3 text-[11px] text-zinc-400">
              <span>{result.rowCount}건 처리</span>
              <span>소요 {result.elapsed}</span>
            </div>
          </div>

          {/* 입력 / 출력 비교 */}
          <div className="grid grid-cols-2 gap-0 divide-x divide-zinc-100">
            <div className="px-4 pb-4">
              <p className="text-[10px] font-medium text-zinc-400 uppercase tracking-wider mb-2">
                Input
              </p>
              <p className="text-xs text-zinc-600 leading-relaxed bg-zinc-50 rounded-lg px-3 py-2.5">
                {result.input}
              </p>
            </div>
            <div className="px-4 pb-4">
              <p className="text-[10px] font-medium text-indigo-400 uppercase tracking-wider mb-2">
                Output
              </p>
              <pre className="text-[11px] text-zinc-700 leading-relaxed bg-indigo-50 rounded-lg px-3 py-2.5 overflow-x-auto whitespace-pre-wrap font-mono">
                {result.output}
              </pre>
            </div>
          </div>

          {/* 파이프라인 안내 */}
          <div className="px-4 py-3 bg-zinc-50 border-t border-zinc-100">
            <p className="text-xs text-zinc-500">
              결과가 정상이라면{" "}
              <span className="font-medium text-indigo-600">파이프라인 탭</span>
              에서 전체 prepare를 실행하세요.
            </p>
          </div>
        </div>
      )}
    </div>
  )
}