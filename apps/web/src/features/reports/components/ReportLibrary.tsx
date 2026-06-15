// 보고서 에디터 좌측 "저장된 결과 보관함" — 검색 + 타입 필터 + 결과 카드 목록.
// 카드는 보고서에 "추가"(+)하거나 캔버스로 드래그할 수 있고, 이미 추가된 결과는 dim + 체크.
import { useMemo, useState } from "react";
import { Check, Library, Plus, Search, Trash2 } from "lucide-react";
import { cn } from "@/lib/utils";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";
import {
  LIB_TYPE_LABEL,
  type LibType,
  type LibraryItem,
} from "../models/editor";

const FILTERS: { label: string; type: LibType | "all" }[] = [
  { label: "전체", type: "all" },
  { label: "차트", type: "chart" },
  { label: "표", type: "table" },
  { label: "원문", type: "text" },
];

export function ReportLibrary({
  items,
  usedIds,
  onAdd,
  onDelete,
  onDragStart,
  onDragEnd,
}: {
  items: LibraryItem[];
  usedIds: Set<string>;
  onAdd: (libId: string) => void;
  onDelete: (libId: string) => void;
  onDragStart: (libId: string) => void;
  onDragEnd: () => void;
}) {
  const [filter, setFilter] = useState<LibType | "all">("all");
  const [query, setQuery] = useState("");

  const counts = useMemo(() => {
    const c: Record<string, number> = { all: items.length };
    for (const l of items) c[l.type] = (c[l.type] ?? 0) + 1;
    return c;
  }, [items]);

  const list = useMemo(() => {
    const q = query.trim().toLowerCase();
    return items
      .filter((l) => filter === "all" || l.type === filter)
      .filter(
        (l) =>
          !q ||
          l.title.toLowerCase().includes(q) ||
          l.question.toLowerCase().includes(q),
      );
  }, [items, filter, query]);

  return (
    <aside className="flex h-full w-80 shrink-0 flex-col border-r border-zinc-200 bg-white">
      <div className="shrink-0 p-4 pb-0">
        <h2 className="flex items-center gap-2 text-[15px] font-extrabold text-zinc-900">
          <Library className="h-4.25 w-4.25 text-zinc-400" />
          저장된 결과 보관함
          <span className="ml-auto text-xs font-bold text-zinc-400">
            {items.length}
          </span>
        </h2>

        <div className="mt-3.5 flex h-9.5 items-center gap-2 rounded-xl border border-zinc-200 px-3 transition focus-within:border-violet-500 focus-within:ring-3 focus-within:ring-violet-100">
          <Search className="h-3.75 w-3.75 shrink-0 text-zinc-400" />
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="결과 검색…"
            className="w-full bg-transparent text-[13.5px] text-zinc-800 outline-none placeholder:text-zinc-400"
          />
        </div>

        <div className="mt-3 flex flex-wrap gap-1.5">
          {FILTERS.map((f) => (
            <button
              key={f.type}
              onClick={() => setFilter(f.type)}
              className={cn(
                "rounded-full border px-3 py-1.25 text-[12.5px] font-semibold transition-colors",
                filter === f.type
                  ? "border-zinc-900 bg-zinc-900 text-white"
                  : "border-zinc-200 bg-white text-zinc-600 hover:border-zinc-300",
              )}
            >
              {f.label}
              <span
                className={cn(
                  "ml-1 font-bold",
                  filter === f.type ? "opacity-60" : "text-zinc-400",
                )}
              >
                {counts[f.type] ?? 0}
              </span>
            </button>
          ))}
        </div>
      </div>

      <div className="flex min-h-0 flex-1 flex-col gap-2 overflow-y-auto p-4 pt-3.5">
        {list.length === 0 ? (
          <div className="py-10 text-center text-[13px] text-zinc-400">
            검색 결과가 없습니다
          </div>
        ) : (
          list.map((l) => {
            const used = usedIds.has(l.id);
            return (
              <div
                key={l.id}
                draggable={!used}
                onDragStart={(e) => {
                  if (used) return;
                  e.dataTransfer.effectAllowed = "copy";
                  try {
                    e.dataTransfer.setData("text/plain", l.id);
                  } catch {
                    /* ignore */
                  }
                  onDragStart(l.id);
                }}
                onDragEnd={onDragEnd}
                className={cn(
                  "flex items-center gap-2.5 rounded-xl border border-zinc-100 bg-white p-2.75 shadow-sm transition hover:border-zinc-200 hover:shadow-md",
                  used && "opacity-55",
                )}
              >
                <div className="min-w-0 flex-1">
                  <div className="truncate text-[13.5px] font-bold text-zinc-900">
                    {l.title}
                  </div>
                  <div className="mt-0.5 truncate text-[11.5px] text-zinc-400">
                    {l.question}
                  </div>
                </div>
                <span className="shrink-0 rounded-md bg-zinc-100 px-1.75 py-0.5 text-[10.5px] font-bold text-zinc-400">
                  {LIB_TYPE_LABEL[l.type]}
                </span>
                <DeleteDialog
                  title="보관함에서 삭제"
                  description={
                    used
                      ? "결과를 보관함에서 완전히 삭제합니다. 보고서에 사용 중이라 해당 블록도 함께 제거됩니다."
                      : "결과를 보관함에서 완전히 삭제합니다."
                  }
                  onDelete={() => onDelete(l.id)}
                  trigger={
                    <button
                      title="보관함에서 삭제"
                      className="grid h-7.5 w-7.5 shrink-0 place-items-center rounded-lg text-zinc-300 transition hover:bg-red-50 hover:text-red-500"
                    >
                      <Trash2 className="h-4 w-4" strokeWidth={2.2} />
                    </button>
                  }
                >
                  <b className="mt-1 text-sm font-semibold text-zinc-800">
                    {l.title}
                  </b>
                </DeleteDialog>
                {used ? (
                  <span
                    title="보고서에서 사용 중"
                    className="grid h-7.5 w-7.5 shrink-0 place-items-center rounded-lg bg-emerald-50 text-emerald-600"
                  >
                    <Check className="h-4 w-4" strokeWidth={2.6} />
                  </span>
                ) : (
                  <button
                    onClick={() => onAdd(l.id)}
                    title="보고서에 추가"
                    className="grid h-7.5 w-7.5 shrink-0 place-items-center rounded-lg bg-violet-50 text-violet-700 transition hover:brightness-95"
                  >
                    <Plus className="h-4 w-4" strokeWidth={2.6} />
                  </button>
                )}
              </div>
            );
          })
        )}
      </div>
    </aside>
  );
}
