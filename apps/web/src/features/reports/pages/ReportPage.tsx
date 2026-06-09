import { useMemo, useState } from "react";
import { CheckSquare, Search } from "lucide-react";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import { cn } from "@/lib/utils";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import {
  MOCK_RESULTS,
  type ReportFormat,
  type ReportResult,
} from "../models/model";
import { ResultCard } from "../components/ResultCard";
import { SelectionBar } from "../components/SelectionBar";
import { ReportDrawer } from "../components/ReportDrawer";

type Filter = "all" | "chart" | "table";

const FILTERS: { value: Filter; label: string }[] = [
  { value: "all", label: "전체" },
  { value: "chart", label: "차트" },
  { value: "table", label: "표" },
];

export default function ReportPage() {
  const { projectId } = useProjectParams();
  const { data: project } = useProjectDetail(projectId);

  const [filter, setFilter] = useState<Filter>("all");
  const [query, setQuery] = useState("");
  // 선택된 결과 id를 "순서 있는 배열"로 관리 → 보고서 구성에서 순서 변경을 반영.
  const [selectedIds, setSelectedIds] = useState<string[]>([]);
  const [drawerOpen, setDrawerOpen] = useState(false);

  const counts = useMemo(
    () => ({
      all: MOCK_RESULTS.length,
      chart: MOCK_RESULTS.filter((r) => r.kind === "chart").length,
      table: MOCK_RESULTS.filter((r) => r.kind === "table").length,
    }),
    [],
  );

  const visible = useMemo(() => {
    const q = query.trim().toLowerCase();
    return MOCK_RESULTS.filter(
      (r) => filter === "all" || r.kind === filter,
    ).filter(
      (r) =>
        !q ||
        r.title.toLowerCase().includes(q) ||
        r.chat.toLowerCase().includes(q),
    );
  }, [filter, query]);

  // 날짜 그룹별로 묶어 라벨과 함께 노출.
  const groups = useMemo(() => {
    const map = new Map<string, ReportResult[]>();
    for (const r of visible) {
      const arr = map.get(r.group) ?? [];
      arr.push(r);
      map.set(r.group, arr);
    }
    return [...map.entries()];
  }, [visible]);

  // 선택 순서를 유지한 항목 목록(보고서 구성 순서의 source of truth).
  const selectedItems = selectedIds
    .map((id) => MOCK_RESULTS.find((r) => r.id === id))
    .filter((r): r is ReportResult => Boolean(r));

  const toggle = (id: string) =>
    setSelectedIds((prev) =>
      prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id],
    );

  const toggleAllVisible = () => {
    const allSelected = visible.every((r) => selectedIds.includes(r.id));
    setSelectedIds((prev) => {
      if (allSelected) {
        const remove = new Set(visible.map((r) => r.id));
        return prev.filter((id) => !remove.has(id));
      }
      const add = visible.map((r) => r.id).filter((id) => !prev.includes(id));
      return [...prev, ...add];
    });
  };

  // 보고서 구성 드로어의 드래그 정렬 — from 위치 항목을 to 위치로 이동.
  const reorder = (from: number, to: number) =>
    setSelectedIds((prev) => {
      const next = [...prev];
      const [moved] = next.splice(from, 1);
      next.splice(to, 0, moved);
      return next;
    });

  const handleDownload = (format: ReportFormat) => {
    // 샘플: 실제 생성은 백엔드 연동 예정. (순서대로 selectedIds 전달)
    console.info("보고서 다운로드", { format, ids: selectedIds });
    setDrawerOpen(false);
  };

  return (
    <div className="p-8 pb-32">
      <div className="mb-6">
        <Breadcrumbs
          items={[
            { label: "프로젝트", to: "/projects" },
            { label: project?.name ?? "프로젝트" },
            { label: "분석 보고서" },
          ]}
        />
      </div>

      <header>
        <h1 className="text-2xl font-extrabold tracking-tight text-zinc-900">
          분석 보고서
        </h1>
        <p className="mt-1.5 text-sm text-zinc-500">
          분석 채팅에서 생성된 차트·표를 모아 보고서로 만듭니다. 항목을 선택해
          원하는 형식으로 내려받으세요.
        </p>
      </header>

      {/* toolbar */}
      <div className="mt-6 flex flex-wrap items-center gap-2.5">
        <div className="inline-flex gap-1 rounded-xl bg-zinc-200/60 p-0.75">
          {FILTERS.map((f) => (
            <button
              key={f.value}
              onClick={() => setFilter(f.value)}
              className={cn(
                "inline-flex items-center gap-1.5 rounded-lg px-3 py-1.5 text-[13px] font-semibold transition-colors",
                filter === f.value
                  ? "bg-white text-zinc-900 shadow-sm"
                  : "text-zinc-500 hover:text-zinc-700",
              )}
            >
              {f.label}
              <span
                className={cn(
                  "text-[11px] font-bold",
                  filter === f.value ? "text-violet-600" : "text-zinc-400",
                )}
              >
                {counts[f.value]}
              </span>
            </button>
          ))}
        </div>

        <button
          onClick={toggleAllVisible}
          className="inline-flex h-9.5 items-center gap-1.5 rounded-xl border border-zinc-200 bg-white px-3 text-[13px] font-semibold text-zinc-600 transition-colors hover:border-zinc-300 hover:text-zinc-900"
        >
          <CheckSquare className="h-4 w-4" />
          전체 선택
        </button>

        <div className="ml-auto inline-flex h-9.5 min-w-56 items-center gap-2 rounded-xl border border-zinc-200 bg-white px-3">
          <Search className="h-4 w-4 text-zinc-400" />
          <input
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            placeholder="결과 제목·채팅 검색"
            className="w-full bg-transparent text-[13.5px] text-zinc-800 outline-none placeholder:text-zinc-400"
          />
        </div>
      </div>

      {/* 결과 그리드 (날짜 그룹) */}
      {groups.length === 0 ? (
        <p className="mt-16 text-center text-sm text-zinc-400">
          조건에 맞는 결과가 없습니다.
        </p>
      ) : (
        groups.map(([group, items]) => (
          <section key={group}>
            <div className="mb-3 mt-7 flex items-center gap-2 text-[12.5px] font-bold text-zinc-400">
              {group}
              <span className="h-px flex-1 bg-zinc-100" />
            </div>
            <div className="grid grid-cols-[repeat(auto-fill,minmax(270px,1fr))] gap-4">
              {items.map((r) => (
                <ResultCard
                  key={r.id}
                  result={r}
                  selected={selectedIds.includes(r.id)}
                  onToggle={() => toggle(r.id)}
                />
              ))}
            </div>
          </section>
        ))
      )}

      <SelectionBar
        count={selectedIds.length}
        onClear={() => setSelectedIds([])}
        onCreate={() => setDrawerOpen(true)}
      />

      <ReportDrawer
        open={drawerOpen}
        onOpenChange={setDrawerOpen}
        items={selectedItems}
        onRemove={toggle}
        onReorder={reorder}
        onDownload={handleDownload}
      />
    </div>
  );
}
