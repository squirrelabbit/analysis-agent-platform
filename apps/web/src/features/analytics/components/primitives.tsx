import type { ReactNode } from "react";
import { Lightbulb } from "lucide-react";
import { cn } from "@/lib/utils";

// 데이터 기초 분석 보고서 공용 UI 조각 (section label / block / unit badge / insight).

// 번호 뱃지 + 제목 + 우측 보조 슬롯(범위 pill 등).
export function SectionLabel({
  no,
  title,
  hint,
  right,
}: {
  no?: number;
  title: ReactNode;
  hint?: ReactNode;
  right?: ReactNode;
}) {
  return (
    <div className="mb-3 mt-8 flex items-baseline gap-2.5 text-[13px] font-bold text-zinc-500">
      {/* <span className="grid h-5 w-5 shrink-0 place-items-center rounded-md bg-violet-50 text-[11.5px] font-extrabold text-violet-600">
        {no}
      </span> */}
      <span>{title}</span>
      {hint && <span className="font-semibold text-zinc-400">{hint}</span>}
      {right && <span className="ml-auto">{right}</span>}
    </div>
  );
}

// 범위(최근 연도/전체 기간) pill.
export function ScopePill({ children }: { children: ReactNode }) {
  return (
    <span className="rounded-full bg-violet-50 px-2.5 py-0.5 text-[11px] font-bold text-violet-600">
      {children}
    </span>
  );
}

// 카드 컨테이너.
export function Block({
  children,
  className,
}: {
  children: ReactNode;
  className?: string;
}) {
  return (
    <div
      className={cn(
        "rounded-2xl border border-zinc-100 bg-white p-5.5 shadow-sm",
        className,
      )}
    >
      {children}
    </div>
  );
}

// 카드 헤더: 제목/부제 + 우측 단위 뱃지.
export function BlockTitle({
  title,
  sub,
  unit,
}: {
  title: ReactNode;
  sub?: ReactNode;
  unit?: "doc" | "clause";
}) {
  return (
    <div className="flex items-start gap-3">
      <div className="min-w-0">
        <div className="text-[15px] font-bold text-zinc-900">{title}</div>
        {sub && (
          <div className="mt-1 text-[12.5px] font-medium text-zinc-400">
            {sub}
          </div>
        )}
      </div>
      {unit && <UnitBadge unit={unit} />}
    </div>
  );
}

// 분석 단위 뱃지 (문서 기준 / 절 기준).
export function UnitBadge({ unit }: { unit: "doc" | "clause" }) {
  const isDoc = unit === "doc";
  return (
    <span
      className={cn(
        "ml-auto inline-flex shrink-0 items-center gap-1.5 rounded-full px-2.75 py-1 text-[11px] font-bold",
        isDoc ? "bg-blue-50 text-blue-600" : "bg-violet-50 text-violet-600",
      )}
    >
      <span
        className={cn(
          "h-1.5 w-1.5 rounded-full",
          isDoc ? "bg-blue-600" : "bg-violet-600",
        )}
      />
      
      {isDoc ? "문서 기준" : "절 기준"}
    </span>
  );
}

// 카드 하단 한 줄 해석. children에 <b>로 강조 텍스트 삽입.
export function CardInsight({ children }: { children: ReactNode }) {
  return (
        <div className="flex gap-2.5 items-center mt-4 text-[13px] rounded-2xl border border-violet-200 bg-violet-50/50 p-4 text-sm">

    {/* <div className="mt-4 flex items-start gap-2.5 border-t border-zinc-100 pt-4 text-[13px] leading-relaxed text-zinc-600 [&_b]:font-bold [&_b]:text-zinc-900"> */}
      <Lightbulb className="mt-0.5 h-4 w-4 shrink-0 text-violet-500" />
      <div>{children}</div>
    {/* </div> */}
    </div>
  );
}

// 표 공용 — 디자인 .dtable 대응.
export function DataTableMini({
  head,
  children,
}: {
  head: ReactNode;
  children: ReactNode;
}) {
  return (
    <div className="overflow-hidden rounded-xl border border-zinc-100">
      <table className="w-full border-collapse text-[12.5px]">
        {head}
        {children}
      </table>
    </div>
  );
}
