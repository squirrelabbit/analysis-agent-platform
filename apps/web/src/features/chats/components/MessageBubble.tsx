import { Pencil } from "lucide-react";
import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";
import ChartView from "./ChartView";
import CollapsibleTable from "./CollapsibleTable";
import DisplayTable from "./DisplayTable";
import EvidenceCardList from "./EvidenceCardList";
import MetricCompareView from "./MetricCompareView";
import MessageWarnings from "./MessageWarnings";
import PlanPanel from "./PlanPanel";
import ResultCardActions from "./ResultCardActions";
import RunStatus from "./RunStatus";
import EditableText from "./EditableText";

// 메시지 하나에 chart/table을 동시에 펼치지 않는다 — recommended_view 기준
// 단일 메인 결과 + 필요 시 상세 데이터 접이식.
//   bar/line + chart 매핑 성공  → chart 메인, 상세 데이터(display) 접이식
//   bar/line 추천했지만 데이터 부족 → chart 생략, 안내 + display 메인
//   recommended_view = table or unknown or 없음 → display 메인
//   display도 없으면 텍스트만
interface MessageBubbleProps {
  message: ChatMessage;
  // 보고서 패널 적재 — assistant 결과 메시지(runId 보유)에만 연결. 핸들러가 없으면
  // 1차 액션만 빠지고 나머지 결과 액션은 그대로(이력 로딩 중·user 메시지 등).
  onAddToReport?: () => void;
  isAdded?: boolean;
  // 편집 가능한 결과 카드 제목 — 패널 카드와 양방향 동기화(같은 cardState 공유).
  // title/onTitleChange가 함께 있을 때만 노출(runId 있는 결과 메시지).
  title?: string;
  onTitleChange?: (value: string) => void;
  // 결과 액션(복사/다운로드/스텁) 피드백 토스트.
  onToast?: (message: string) => void;
}

export default function MessageBubble({
  message,
  onAddToReport,
  isAdded = false,
  title,
  onTitleChange,
  onToast,
}: MessageBubbleProps) {
  const isUser = message.role === "user";

  const display = !isUser ? message.display : undefined;
  const chart = !isUser ? message.chart : undefined;
  const metric = !isUser ? message.metric : undefined;
  const evidence = !isUser ? message.evidence : undefined;
  const hasPlan = !isUser && !!message.plan;
  const hasWarnings =
    !isUser &&
    ((message.warnings && message.warnings.length > 0) ||
      (!!message.taxonomyStatus && message.taxonomyStatus !== "ok"));

  // 메인 결과 1개 선택: metric > evidence > chart > table.
  const metricMain = !!metric;
  const evidenceMain = !metricMain && !!evidence;
  const chartMain = !metricMain && !evidenceMain && !!chart;
  const tableMain = !metricMain && !evidenceMain && !chartMain && !!display;
  const hasNonTableMain = metricMain || evidenceMain || chartMain;
  const showFallbackNotice =
    !isUser && !chartMain && message.chartFallbackReason === "insufficient_data";

  const isWide = hasNonTableMain || tableMain || hasPlan;

  // 결과 액션바는 실제 결과(표/차트/지표/원문)가 있는 assistant 메시지에만.
  // 텍스트만 있는 거절/안내 응답엔 액션이 의미 없어 노출하지 않는다.
  const hasResult = !!display || !!chart || !!metric || !!evidence;
  const showActions = !isUser && hasResult;

  return (
    <div className={cn("flex gap-2.5 items-start", isUser && "flex-row-reverse")}>
      <Avatar className="h-7 w-7 shrink-0 mt-0.5">
        <AvatarFallback
          className={cn(
            "text-[10px]",
            isUser
              ? "bg-zinc-200 text-zinc-600"
              : "bg-violet-100 text-violet-700",
          )}
        >
          {isUser ? "나" : "AI"}
        </AvatarFallback>
      </Avatar>
      <div
        className={cn(
          "rounded-2xl px-4 py-3 text-sm",
          isWide ? "max-w-full flex-1 min-w-0" : "max-w-[80%]",
          isUser
            ? "bg-violet-600 text-white rounded-tr-sm"
            : "bg-white border border-zinc-100 text-zinc-800 rounded-tl-sm",
        )}
      >
        <div className="whitespace-pre-wrap break-words">{message.content}</div>

        {!isUser && (
          <RunStatus status={message.runStatus} error={message.runError} />
        )}

        {showFallbackNotice && (
          <p className="mt-2 text-[11px] text-zinc-500">
            차트로 표시하기에는 유효한 비교값이 부족하여 표로 표시합니다.
          </p>
        )}

        {/* 편집 가능한 결과 카드 제목 — 패널/보고서와 공유. 결과 메시지에만. */}
        {hasResult && onTitleChange && title !== undefined && (
          <div className="group mt-2 flex items-center gap-1.5">
            <EditableText
              value={title}
              onChange={onTitleChange}
              ariaLabel="결과 카드 제목"
              className="min-w-0 flex-1 cursor-text rounded-md px-1.5 py-0.5 text-[13.5px] font-bold leading-snug text-zinc-900 hover:bg-zinc-50 focus:bg-white focus:ring-2 focus:ring-violet-200"
            />
            <Pencil className="h-3.25 w-3.25 shrink-0 text-zinc-300 opacity-0 transition group-hover:opacity-100" />
          </div>
        )}

        {/* 메인 결과 — metric > evidence > chart > table */}
        {metricMain && metric && <MetricCompareView metric={metric} />}
        {evidenceMain && evidence && <EvidenceCardList evidence={evidence} />}
        {chartMain && chart && <ChartView chart={chart} />}
        {tableMain && display && <DisplayTable display={display} />}

        {/* 메인이 table이 아니면 display는 상세 데이터 접이식으로 */}
        {hasNonTableMain && display && <CollapsibleTable display={display} />}

        {hasPlan && <PlanPanel plan={message.plan!} />}

        {hasWarnings && (
          <MessageWarnings
            warnings={message.warnings}
            taxonomyStatus={message.taxonomyStatus}
          />
        )}

        {showActions && (
          <ResultCardActions
            message={message}
            onAddToReport={onAddToReport}
            isAdded={isAdded}
            onToast={onToast ?? (() => {})}
          />
        )}
      </div>
    </div>
  );
}
