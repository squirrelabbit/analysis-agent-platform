import { Fragment, type ReactNode, useState } from "react";
import { Button } from "@/components/ui/button";
import { cn } from "@/lib/utils";

export type Column<T> = {
  /** thead 헤더 내용 */
  header: ReactNode;
  /** th에 적용할 추가 className (너비 등) */
  headerClassName?: string;
  /** 행 렌더러 — 완성된 <td>를 반환한다 (셀별 className/상호작용을 호출부가 제어) */
  cell: (item: T) => ReactNode;
};

type DataTableProps<T> = {
  columns: Column<T>[];
  items: T[] | undefined;
  rowKey: (item: T) => string;
  /** 헤더 좌측 영역 (건수/제목 등) */
  title: ReactNode;
  /** 헤더 우측 영역 (필터 컨트롤 등) */
  toolbar?: ReactNode;
  emptyText?: string;
  page: number;
  totalPages: number;
  totalCount: number;
  onPageChange: (page: number) => void;
};

/** 빌드 결과 탭의 공통 표: 필터 헤더 + 테이블 + 페이지네이션 푸터 */
export function DataTable<T>({
  columns,
  items,
  rowKey,
  title,
  toolbar,
  emptyText = "해당 항목이 없습니다",
  page,
  totalPages,
  totalCount,
  onPageChange,
}: DataTableProps<T>) {
  return (
    <div className="rounded-xl border border-zinc-100 bg-white overflow-hidden">
      <div className="px-4 py-3 border-b border-zinc-50 flex items-center justify-between flex-wrap gap-2">
        <span className="text-xs font-medium text-zinc-500">{title}</span>
        {toolbar && (
          <div className="flex items-center gap-1.5 flex-wrap">{toolbar}</div>
        )}
      </div>
      <div className="overflow-x-auto">
        {/* table-fixed: 컬럼 폭을 내용(필터로 바뀌는 행)에 의존하지 않게 고정.
            auto-layout이면 필터마다 셀 내용 길이가 달라져 테이블/컬럼 폭이 출렁인다. */}
        <table className="w-full table-fixed text-sm">
          <thead>
            <tr className="border-b border-zinc-50">
              {columns.map((col, i) => (
                <th
                  key={i}
                  className={cn(
                    "text-left px-4 py-2.5 text-xs font-medium text-zinc-400 uppercase tracking-wide",
                    col.headerClassName,
                  )}
                >
                  {col.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {!items || items.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length}
                  className="text-center py-8 text-sm text-zinc-400"
                >
                  {emptyText}
                </td>
              </tr>
            ) : (
              items.map((item) => (
                <tr
                  key={rowKey(item)}
                  className="hover:bg-zinc-50/60 transition-colors"
                >
                  {columns.map((col, i) => (
                    <Fragment key={i}>{col.cell(item)}</Fragment>
                  ))}
                </tr>
              ))
            )}
          </tbody>
        </table>
        <div className="flex items-center justify-between px-4 py-3 border-t border-zinc-100">
          <p className="text-xs text-zinc-400">총 {totalCount}개</p>
          <div className="flex items-center gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={page === 1}
              onClick={() => onPageChange(page - 1)}
            >
              이전
            </Button>
            <span className="text-xs text-zinc-500">
              {page} / {totalPages}
            </span>
            <Button
              size="sm"
              variant="outline"
              disabled={page === totalPages}
              onClick={() => onPageChange(page + 1)}
            >
              다음
            </Button>
          </div>
        </div>
      </div>
    </div>
  );
}

/** 헤더 우측에 쓰는 pill 형태 필터 버튼 그룹 */
export function FilterPills({
  options,
  value,
  onChange,
}: {
  options: { label: string; value: string }[];
  value: string;
  onChange: (value: string) => void;
}) {
  return (
    <>
      {options.map((opt) => (
        <button
          key={opt.value}
          onClick={() => onChange(opt.value)}
          className={cn(
            "px-2.5 py-1 rounded-full text-xs font-medium border transition-colors",
            value === opt.value
              ? "bg-zinc-800 text-white border-zinc-800"
              : "bg-white text-zinc-600 border-zinc-200 hover:bg-zinc-50",
          )}
        >
          {opt.label}
        </button>
      ))}
    </>
  );
}

/** 클릭하면 2줄 클램프 ↔ 전체 표시를 토글하는 텍스트 셀 */
export function ExpandableTextCell({ text }: { text: string }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <td
      onClick={() => setExpanded((v) => !v)}
      title={expanded ? "접기" : "더보기"}
      className="px-4 py-3 text-xs text-zinc-500 leading-relaxed max-w-sm cursor-pointer hover:bg-zinc-50/60 transition-colors"
    >
      <p className={cn(!expanded && "line-clamp-2")}>{text}</p>
    </td>
  );
}
