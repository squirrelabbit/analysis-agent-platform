import { AlertTriangle, Check } from "lucide-react";
import { cn } from "@/lib/utils";

// 토스트 톤 — success(그린 체크, 결과 액션 피드백) / warn(앰버 경고, 안내·실패).
export type ToastTone = "success" | "warn";

// 디자인 「결과 카드 액션 아이콘」 토스트 — 하단 중앙 다크 칩.
// 결과 액션(복사/다운로드/저장) 피드백 + 안내/실패 경고용. (silverone 2026-06-11)
export default function ChatToast({
  message,
  visible,
  tone = "success",
}: {
  message: string;
  visible: boolean;
  tone?: ToastTone;
}) {
  const Icon = tone === "warn" ? AlertTriangle : Check;
  return (
    <div
      className={cn(
        "fixed bottom-[26px] left-1/2 z-50 flex items-center gap-2 rounded-xl",
        "bg-[#16161c] px-[18px] py-[11px] text-[13px] font-semibold text-white shadow-lg",
        "transition-all duration-200",
        visible
          ? "-translate-x-1/2 translate-y-0 opacity-100"
          : "pointer-events-none -translate-x-1/2 translate-y-3.5 opacity-0",
      )}
      role="status"
      aria-live="polite"
    >
      <Icon
        className={cn(
          "h-4 w-4",
          tone === "warn" ? "text-amber-400" : "text-emerald-400",
        )}
        strokeWidth={2.4}
      />
      <span>{message}</span>
    </div>
  );
}
