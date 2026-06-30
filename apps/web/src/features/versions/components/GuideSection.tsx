import { ChevronRight } from "lucide-react";
import type { ReactNode } from "react";

// 빌드 결과 탭 공용 "안내" 카드 — 기본 접힘 details + ChevronRight summary.
// 절 유형(Aspect) 안내 / 진성 분류 기준 안내가 동일 템플릿을 공유한다.
export function GuideSection({
  title,
  meta,
  children,
}: {
  title: string;
  meta?: ReactNode;
  children: ReactNode;
}) {
  return (
    <details className="group rounded-2xl border border-zinc-100 bg-white px-5 py-3.5 shadow-sm">
      <summary className="flex cursor-pointer list-none items-center gap-1.5 text-sm font-bold text-zinc-900 marker:hidden">
        <ChevronRight className="h-4 w-4 text-zinc-400 transition-transform group-open:rotate-90" />
        {title}
        {meta && <span className="font-medium text-zinc-400">{meta}</span>}
      </summary>
      <div className="mt-3.5">{children}</div>
    </details>
  );
}
