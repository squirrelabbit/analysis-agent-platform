import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";
import ChartView from "./ChartView";
import CollapsibleTable from "./CollapsibleTable";
import DisplayTable from "./DisplayTable";
import MessageWarnings from "./MessageWarnings";
import PlanPanel from "./PlanPanel";

// 메시지 하나에 chart/table을 동시에 펼치지 않는다 — recommended_view 기준
// 단일 메인 결과 + 필요 시 상세 데이터 접이식.
//   bar/line + chart 매핑 성공  → chart 메인, 상세 데이터(display) 접이식
//   bar/line 추천했지만 데이터 부족 → chart 생략, 안내 + display 메인
//   recommended_view = table or unknown or 없음 → display 메인
//   display도 없으면 텍스트만
export default function MessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";

  const display = !isUser ? message.display : undefined;
  const chart = !isUser ? message.chart : undefined;
  const hasPlan = !isUser && !!message.plan;
  const hasWarnings =
    !isUser &&
    ((message.warnings && message.warnings.length > 0) ||
      (!!message.taxonomyStatus && message.taxonomyStatus !== "ok"));

  const chartMain = !!chart;
  const tableMain = !chartMain && !!display;
  const showFallbackNotice =
    !isUser && !chartMain && message.chartFallbackReason === "insufficient_data";

  const isWide = chartMain || tableMain || hasPlan;

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

        {showFallbackNotice && (
          <p className="mt-2 text-[11px] text-zinc-500">
            차트로 표시하기에는 유효한 비교값이 부족하여 표로 표시합니다.
          </p>
        )}

        {/* 메인 결과 — chart 우선, 없으면 table */}
        {chartMain && <ChartView chart={chart} />}
        {tableMain && <DisplayTable display={display} />}

        {/* chart가 메인일 때 display는 상세 데이터 접이식 */}
        {chartMain && display && <CollapsibleTable display={display} />}

        {hasPlan && <PlanPanel plan={message.plan!} />}

        {hasWarnings && (
          <MessageWarnings
            warnings={message.warnings}
            taxonomyStatus={message.taxonomyStatus}
          />
        )}
      </div>
    </div>
  );
}
