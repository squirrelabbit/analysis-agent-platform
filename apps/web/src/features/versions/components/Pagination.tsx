import {
  ChevronLeft,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
} from "lucide-react";
import { cn } from "@/lib/utils";

type PaginationProps = {
  page: number;
  totalPages: number;
  /** 좌측에 노출할 전체 건수 (옵션) */
  totalCount?: number;
  onPageChange: (page: number) => void;
};

// <<, >> 점프 단위 (페이지 수)
const JUMP = 10;

/**
 * 표시할 페이지 번호 목록. 양끝(1, last)과 현재 페이지 ±2(가운데 최대 5개)만 노출하고
 * 사이가 떨어지면 "…"로 생략한다. 예: 1 … 4 5 6 7 8 … 37
 */
function pageItems(current: number, total: number): (number | "ellipsis")[] {
  if (total <= 9) {
    return Array.from({ length: total }, (_, i) => i + 1);
  }
  const items: (number | "ellipsis")[] = [1];
  const left = Math.max(2, current - 2);
  const right = Math.min(total - 1, current + 2);
  if (left > 2) items.push("ellipsis");
  for (let i = left; i <= right; i++) items.push(i);
  if (right < total - 1) items.push("ellipsis");
  items.push(total);
  return items;
}

const NAV_BTN =
  "grid h-8 w-8 place-items-center rounded-lg text-zinc-400 transition-colors hover:bg-zinc-100 disabled:pointer-events-none disabled:opacity-40";

/** 숫자 클릭형 페이지네이션. 생략(…) 표시 + 단일/10페이지 점프 화살표. */
export function Pagination({ page, totalPages, onPageChange }: PaginationProps) {
  const items = pageItems(page, totalPages);
  const go = (p: number) => {
    const clamped = Math.min(Math.max(p, 1), totalPages);
    if (clamped !== page) onPageChange(clamped);
  };

  return (
    <div className="flex items-center place-content-center gap-1 px-4 py-3">
      <button
        type="button"
        aria-label="이전 10페이지"
        disabled={page === 1}
        onClick={() => go(page - JUMP)}
        className={NAV_BTN}
      >
        <ChevronsLeft className="h-4 w-4" />
      </button>
      <button
        type="button"
        aria-label="이전 페이지"
        disabled={page === 1}
        onClick={() => go(page - 1)}
        className={NAV_BTN}
      >
        <ChevronLeft className="h-4 w-4" />
      </button>
      {items.map((it, i) =>
        it === "ellipsis" ? (
          <span
            key={`e${i}`}
            className="grid h-8 w-8 place-items-center text-xs text-zinc-300"
          >
            …
          </span>
        ) : (
          <button
            key={it}
            type="button"
            aria-current={it === page ? "page" : undefined}
            onClick={() => go(it)}
            className={cn(
              "grid h-8 min-w-8 place-items-center rounded-lg px-2 text-[13px] font-semibold tabular-nums transition-colors",
              it === page
                ? "bg-violet-100 text-violet-700"
                : "text-zinc-500 hover:bg-zinc-100",
            )}
          >
            {it}
          </button>
        ),
      )}
      <button
        type="button"
        aria-label="다음 페이지"
        disabled={page === totalPages}
        onClick={() => go(page + 1)}
        className={NAV_BTN}
      >
        <ChevronRight className="h-4 w-4" />
      </button>
      <button
        type="button"
        aria-label="다음 10페이지"
        disabled={page === totalPages}
        onClick={() => go(page + JUMP)}
        className={NAV_BTN}
      >
        <ChevronsRight className="h-4 w-4" />
      </button>
    </div>
  );
}
