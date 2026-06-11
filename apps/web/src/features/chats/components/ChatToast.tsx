import { Check } from "lucide-react";
import { cn } from "@/lib/utils";

// 디자인 「결과 카드 액션 아이콘」 토스트 — 하단 중앙 다크 칩 + 그린 체크.
// 결과 액션(복사/다운로드/저장) 피드백용. (silverone 2026-06-11)
export default function ChatToast({
  message,
  visible,
}: {
  message: string;
  visible: boolean;
}) {
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
      <Check className="h-4 w-4 text-emerald-400" strokeWidth={2.4} />
      <span>{message}</span>
    </div>
  );
}
