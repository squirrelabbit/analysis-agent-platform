import {
  Fragment,
  type ReactNode,
  useLayoutEffect,
  useRef,
  useState,
} from "react";
import { Check, ChevronDown, ChevronUp, Copy } from "lucide-react";
import { cn } from "@/lib/utils";
import { Pagination } from "./Pagination";

// 스켈레톤 셀 바 폭 — 컬럼마다 다르게 줘서 실제 표처럼 보이게.
const SKELETON_WIDTHS = ["55%", "85%", "45%", "60%", "70%"];

/** 로딩 중 표 본문을 대체하는 스켈레톤 행들 */
function SkeletonRows({ rows, cols }: { rows: number; cols: number }) {
  return (
    <>
      {Array.from({ length: rows }).map((_, r) => (
        <tr key={r}>
          {Array.from({ length: cols }).map((_, c) => (
            <td key={c} className="px-4 py-3.5">
              <div
                className="h-3.5 animate-pulse rounded bg-zinc-100"
                style={{ width: SKELETON_WIDTHS[c % SKELETON_WIDTHS.length] }}
              />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

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
  /** 페이지/필터 변경으로 API 재호출 중일 때 true → 표 위에 로딩 오버레이 */
  loading?: boolean;
  /** 행 그룹 키. 주면 같은 키의 연속 행 앞에 그룹 헤더 행을 끼워 넣는다(예: 같은 문서).
   *  items는 호출부에서 같은 키끼리 인접하도록 정렬돼 있어야 한다. */
  groupBy?: (item: T) => string;
  /** 그룹 헤더 내용 렌더러(groupBy와 함께). count=그 그룹의 행 수, firstItem=첫 행. */
  renderGroupHeader?: (groupKey: string, count: number, firstItem: T) => ReactNode;
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
  loading = false,
  groupBy,
  renderGroupHeader,
}: DataTableProps<T>) {
  // 그룹 헤더에 표시할 그룹별 행 수(현재 페이지 기준).
  const groupCounts = new Map<string, number>();
  if (groupBy && items) {
    for (const it of items) {
      const k = groupBy(it);
      groupCounts.set(k, (groupCounts.get(k) ?? 0) + 1);
    }
  }
  const grouped = !!groupBy && !!renderGroupHeader;

  return (
    <div className="rounded-2xl border border-zinc-100 bg-white shadow-sm overflow-hidden">
      <div className="px-4 py-3 border-b border-zinc-50 flex items-center justify-between flex-wrap gap-2">
        <div className="flex gap-2 items-center">
          <div className="text-[15px] font-bold text-zinc-900">{title}</div>
          <span className="text-xs text-zinc-400">
            {totalCount.toLocaleString()}건{" "}
          </span>
        </div>
        {toolbar && (
          <div className="flex items-center gap-1.5 flex-wrap">{toolbar}</div>
        )}
      </div>
      <div className="overflow-x-auto" aria-busy={loading}>
        {/* table-fixed: 컬럼 폭을 내용(필터로 바뀌는 행)에 의존하지 않게 고정.
            auto-layout이면 필터마다 셀 내용 길이가 달라져 테이블/컬럼 폭이 출렁인다. */}
        <table className="w-full table-fixed text-sm">
          <thead>
            <tr className="border-b border-zinc-100 bg-zinc-50/70">
              {columns.map((col, i) => (
                <th
                  key={i}
                  className={cn(
                    "text-left px-4 py-3 text-xs font-semibold text-zinc-500",
                    col.headerClassName,
                  )}
                >
                  {col.header}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-zinc-50">
            {loading ? (
              // 로딩(페이징/필터 재호출) 중: 이전 행 수만큼 스켈레톤 행 표시 → 높이 유지.
              <SkeletonRows
                rows={items?.length || 8}
                cols={columns.length}
              />
            ) : !items || items.length === 0 ? (
              <tr>
                <td
                  colSpan={columns.length}
                  className="text-center py-8 text-sm text-zinc-400"
                >
                  {emptyText}
                </td>
              </tr>
            ) : (
              items.map((item, idx) => {
                const row = (
                  <tr
                    key={rowKey(item)}
                    className="hover:bg-zinc-50/60 transition-colors"
                  >
                    {columns.map((col, i) => (
                      <Fragment key={i}>{col.cell(item)}</Fragment>
                    ))}
                  </tr>
                );
                if (!grouped) return row;
                // 그룹 첫 행이면(이전 행과 키가 다르면) 앞에 그룹 헤더 행을 끼운다.
                // 두 번째 그룹부터는 위쪽 여백(pt-5)으로 문서 사이를 띄운다.
                const key = groupBy!(item);
                const prevKey = idx > 0 ? groupBy!(items[idx - 1]) : null;
                const isStart = key !== prevKey;
                return (
                  <Fragment key={rowKey(item)}>
                    {isStart && (
                      <tr>
                        <td
                          colSpan={columns.length}
                          className={cn("px-3", idx > 0 && "pt-5")}
                        >
                          {renderGroupHeader!(
                            key,
                            groupCounts.get(key) ?? 1,
                            item,
                          )}
                        </td>
                      </tr>
                    )}
                    {row}
                  </Fragment>
                );
              })
            )}
          </tbody>
        </table>
      </div>
      <Pagination
        page={page}
        totalPages={totalPages}
        totalCount={totalCount}
        onPageChange={onPageChange}
      />
    </div>
  );
}

/** 헤더 우측에 쓰는 pill 형태 필터 버튼 그룹 */
export function FilterPills({
  options,
  value,
  onChange,
  selectedClassName,
}: {
  options: { label: string; value: string }[];
  value: string;
  onChange: (value: string) => void;
  /** 선택된 옵션의 색을 값별로 다르게 주고 싶을 때(예: 감성). 없으면 기본 검정 pill. */
  selectedClassName?: (value: string) => string | undefined;
}) {
  return (
    <>
      {options.map((opt) => {
        const selected = value === opt.value;
        return (
          <button
            key={opt.value}
            onClick={() => onChange(opt.value)}
            className={cn(
              "px-2.5 py-1 rounded-full text-xs font-medium border transition-colors",
              selected
                ? (selectedClassName?.(opt.value) ??
                  "bg-zinc-800 text-white border-zinc-800")
                : "bg-white text-zinc-600 border-zinc-200 hover:bg-zinc-50",
            )}
          >
            {opt.label}
          </button>
        );
      })}
    </>
  );
}

/**
 * 클립보드 복사. navigator.clipboard는 보안 컨텍스트(https/localhost)에서만
 * 동작하므로, 배포 환경(http)까지 일관되게 쓰기 위해 execCommand("copy")로 통일한다.
 * http / localhost / https 모두 동일하게 동작.
 */
function copyToClipboard(text: string): boolean {
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    // 화면 밖 + 스크롤 점프 방지로 배치 후 선택해 복사.
    ta.style.position = "fixed";
    ta.style.top = "-9999px";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    ta.setSelectionRange(0, text.length);
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}

/** 잘리는 문서 ID를 표시하고 클릭하면 전체 ID를 클립보드로 복사하는 셀 */
export function DocIdCell({ id }: { id: string }) {
  const [copied, setCopied] = useState(false);
  const copy = () => {
    if (!copyToClipboard(id)) return;
    setCopied(true);
    setTimeout(() => setCopied(false), 1200);
  };
  return (
    <td className="px-4 py-3">
      <button
        type="button"
        onClick={copy}
        title={copied ? "복사됨" : `클릭하여 복사: ${id}`}
        className="group flex w-full min-w-0 items-center gap-1.5 text-left"
      >
        <span className="truncate text-xs text-zinc-400">{id}</span>
        {copied ? (
          <Check className="h-3.5 w-3.5 shrink-0 text-emerald-500" />
        ) : (
          <Copy className="h-3.5 w-3.5 shrink-0 text-zinc-300 transition-colors group-hover:text-zinc-500" />
        )}
      </button>
    </td>
  );
}

/**
 * 2줄을 넘쳐 잘릴 때만 위/아래 화살표로 펼침/접힘을 토글하는 텍스트 셀.
 * 텍스트가 너비를 넘지 않으면(2줄 이내) 화살표를 표시하지 않는다.
 * 오버플로 판정: line-clamp 상태에서 scrollHeight > clientHeight 비교.
 */
export function ExpandableTextCell({ text }: { text: string }) {
  const pRef = useRef<HTMLParagraphElement>(null);
  const [expanded, setExpanded] = useState(false);
  const [overflowing, setOverflowing] = useState(false);

  // clamp(접힘) 상태에서만 정확히 측정 가능 → collapsed일 때 측정하고,
  // 폭 변화(ResizeObserver)·텍스트 변경 시 다시 측정한다.
  useLayoutEffect(() => {
    const el = pRef.current;
    if (!el || expanded) return;
    const measure = () =>
      setOverflowing(el.scrollHeight - el.clientHeight > 1);
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(el);
    return () => ro.disconnect();
  }, [text, expanded]);

  return (
    <td className="max-w-sm px-4 py-3 align-top text-xs leading-relaxed text-zinc-500">
      <div className="flex items-start gap-1.5">
        <p
          ref={pRef}
          onClick={() => overflowing && setExpanded((v) => !v)}
          className={cn(
            "min-w-0 flex-1 whitespace-pre-line",
            !expanded && "line-clamp-2",
            overflowing && "cursor-pointer",
          )}
        >
          {text}
        </p>
        {overflowing && (
          <button
            type="button"
            onClick={() => setExpanded((v) => !v)}
            title={expanded ? "접기" : "더보기"}
            aria-label={expanded ? "접기" : "더보기"}
            className="mt-px grid h-5 w-5 shrink-0 place-items-center rounded text-zinc-400 transition-colors hover:bg-zinc-100 hover:text-zinc-600"
          >
            {expanded ? (
              <ChevronUp className="h-3.5 w-3.5" />
            ) : (
              <ChevronDown className="h-3.5 w-3.5" />
            )}
          </button>
        )}
      </div>
    </td>
  );
}
