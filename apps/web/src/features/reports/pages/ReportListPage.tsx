// 보고서 목록(CRUD) — 레일 '보고서' → 이 목록 → 카드 클릭 → 에디터(/reports/:id).
// 디자인(보고서 목록.html) 참고. 백엔드 ReportSummary에 있는 필드만 사용:
// 제목 · 블록 수 · 수정/생성 시각. (status/dataset/kind는 백엔드에 없어 미사용.)
import { useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import {
  ArrowUpDown,
  Check,
  ChevronDown,
  Copy,
  FileText,
  MoreVertical,
  Pencil,
  Plus,
  Search,
  SquarePen,
  Trash2,
} from "lucide-react";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { useReports } from "../hooks/reportDoc.query";
import {
  useCreateReport,
  useDeleteReport,
  useDuplicateReport,
  useRenameReport,
} from "../hooks/reportDoc.mutation";
import type { ReportSummary } from "../models";

type SortKey = "updated" | "created" | "name" | "blocks";

const SORT_OPTIONS: { key: SortKey; label: string }[] = [
  { key: "updated", label: "최근 수정순" },
  { key: "created", label: "최근 생성순" },
  { key: "name", label: "이름순 (가나다)" },
  { key: "blocks", label: "블록 많은순" },
];

// ISO 시각 → "N분/시간/일 전" 상대 표기.
function relTime(iso: string): string {
  if (!iso) return "";
  const t = new Date(iso).getTime();
  if (Number.isNaN(t)) return "";
  const min = Math.floor((Date.now() - t) / 60000);
  if (min < 1) return "방금 전";
  if (min < 60) return `${min}분 전`;
  const h = Math.floor(min / 60);
  if (h < 24) return `${h}시간 전`;
  const d = Math.floor(h / 24);
  if (d < 7) return `${d}일 전`;
  if (d < 30) return `${Math.floor(d / 7)}주 전`;
  return `${Math.floor(d / 30)}개월 전`;
}

export default function ReportListPage() {
  const navigate = useNavigate();
  const { projectId } = useProjectParams();
  const { data: project } = useProjectDetail(projectId);
  const { data: reports = [], isLoading } = useReports(projectId);

  const [query, setQuery] = useState("");
  const [sort, setSort] = useState<SortKey>("updated");
  const [createOpen, setCreateOpen] = useState(false);

  const createReport = useCreateReport(projectId);
  const renameReport = useRenameReport(projectId);
  const duplicateReport = useDuplicateReport(projectId);
  const deleteReport = useDeleteReport(projectId);

  const [toast, setToast] = useState<string | null>(null);
  const showToast = (msg: string) => {
    setToast(msg);
    window.setTimeout(
      () => setToast((cur) => (cur === msg ? null : cur)),
      1900,
    );
  };

  const list = useMemo(() => {
    const q = query.trim().toLowerCase();
    const filtered = q
      ? reports.filter((r) => r.title.toLowerCase().includes(q))
      : reports.slice();
    filtered.sort((a, b) => {
      if (sort === "name") return a.title.localeCompare(b.title, "ko");
      if (sort === "blocks") return b.blockCount - a.blockCount;
      if (sort === "created") return cmpDesc(a.createdAt, b.createdAt);
      return cmpDesc(a.updatedAt, b.updatedAt);
    });
    return filtered;
  }, [reports, query, sort]);

  const openReport = (reportId: string) => navigate(reportId);

  const handleCreate = (title: string) => {
    createReport.mutate(
      { title: title.trim() || undefined, blocks: [] },
      {
        onSuccess: (created) => {
          setCreateOpen(false);
          navigate(created.report_id);
        },
        onError: () => showToast("보고서 생성에 실패했어요"),
      },
    );
  };

  const handleRename = (reportId: string, title: string) =>
    renameReport.mutate(
      { reportId, title: title.trim() },
      {
        onSuccess: () => showToast("이름을 변경했습니다"),
        onError: () => showToast("이름 변경에 실패했어요"),
      },
    );

  const handleDuplicate = (r: ReportSummary) =>
    duplicateReport.mutate(
      { reportId: r.reportId, title: `${r.title} (복사본)` },
      {
        onSuccess: () => showToast("복제했습니다"),
        onError: () => showToast("복제에 실패했어요"),
      },
    );

  const handleDelete = (r: ReportSummary) =>
    deleteReport.mutate(r.reportId, {
      onSuccess: () => showToast("보고서를 삭제했습니다"),
      onError: () => showToast("삭제에 실패했어요"),
    });

  const noResults = !isLoading && list.length === 0 && query !== "";

  return (
    <div className="h-full overflow-y-auto">
      <div className="mx-auto max-w-[1180px] px-9 py-8 pb-20">
        <Breadcrumbs
          items={[
            { label: "프로젝트", to: "/projects" },
            { label: project?.name ?? "프로젝트" },
            { label: "보고서" },
          ]}
        />

        {/* head */}
        <div className="mt-4 flex flex-wrap items-end justify-between gap-4">
          <div>
            <div className="flex items-end gap-3">
              <h1 className="text-[26px] font-extrabold tracking-tight text-zinc-900">
                보고서
              </h1>
              <span className="pb-1 text-sm font-bold text-zinc-400">
                {reports.length}개
              </span>
            </div>
            <p className="mt-1.5 text-sm text-zinc-500">
              채팅 분석 결과를 모아 만든 보고서입니다. 카드를 눌러 편집하거나,
              우상단 메뉴에서 이름 변경·복제·삭제할 수 있습니다.
            </p>
          </div>
          <Button onClick={() => setCreateOpen(true)} className="h-9.5 gap-1.5">
            <Plus className="h-4 w-4" strokeWidth={2.2} />새 보고서
          </Button>
        </div>

        {/* controls */}
        <div className="mt-5 flex flex-wrap items-center gap-3">
          <div className="flex h-10 w-70 items-center gap-2 rounded-xl border border-zinc-200 bg-white px-3 transition focus-within:border-violet-500 focus-within:ring-3 focus-within:ring-violet-100">
            <Search className="h-4 w-4 shrink-0 text-zinc-400" />
            <input
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              placeholder="보고서 검색…"
              className="w-full bg-transparent text-sm text-zinc-800 outline-none placeholder:text-zinc-400"
            />
          </div>
          <div className="flex-1" />
          <SortMenu sort={sort} onChange={setSort} />
        </div>

        {/* grid / list */}
        {isLoading ? (
          <div className="py-20 text-center text-sm text-zinc-400">
            보고서를 불러오는 중…
          </div>
        ) : noResults ? (
          <EmptyState />
        ) : (
          <div className="mt-5 grid grid-cols-1 gap-2.5">
            {list.map((r) => (
              <ReportCard
                key={r.reportId}
                report={r}
                onOpen={() => openReport(r.reportId)}
                onRename={(title) => handleRename(r.reportId, title)}
                onDuplicate={() => handleDuplicate(r)}
                onDelete={() => handleDelete(r)}
              />
            ))}

            {query === "" && (
              <button
                onClick={() => setCreateOpen(true)}
                className="flex items-center justify-center gap-2.5 rounded-2xl border-2 border-dashed border-zinc-300 p-4 text-zinc-400 transition hover:border-violet-500 hover:bg-violet-50 hover:text-violet-700"
              >
                <span className="grid h-11.5 w-11.5 place-items-center rounded-2xl border border-zinc-100 bg-white shadow-sm">
                  <Plus className="h-5.5 w-5.5" strokeWidth={2} />
                </span>
                <span className="flex flex-col items-center">
                  <b className="text-sm font-bold">새 보고서</b>
                  <small className="text-xs">빈 보고서로 시작</small>
                </span>
              </button>
            )}
          </div>
        )}
      </div>

      <CreateDialog
        open={createOpen}
        onOpenChange={setCreateOpen}
        onCreate={handleCreate}
        pending={createReport.isPending}
      />

      {/* toast */}
      {toast && (
        <div className="fixed bottom-6 left-1/2 z-80 flex -translate-x-1/2 items-center gap-2.5 rounded-xl bg-zinc-900 px-4.5 py-2.75 text-[13.5px] font-semibold text-white shadow-2xl">
          <Check className="h-4 w-4 text-emerald-400" strokeWidth={2.4} />
          {toast}
        </div>
      )}
    </div>
  );
}

// 수정/생성 시각 비교(내림차순). ISO 문자열 → 시간값.
function cmpDesc(a: string, b: string): number {
  return new Date(b).getTime() - new Date(a).getTime();
}

function SortMenu({
  sort,
  onChange,
}: {
  sort: SortKey;
  onChange: (s: SortKey) => void;
}) {
  const label = SORT_OPTIONS.find((o) => o.key === sort)?.label ?? "";
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button className="inline-flex h-10 items-center gap-2 rounded-xl border border-zinc-200 bg-white px-3 text-[13px] font-semibold text-zinc-600 transition hover:border-zinc-300 hover:text-zinc-900">
          <ArrowUpDown className="h-3.75 w-3.75" />
          {label}
          <ChevronDown className="h-3.5 w-3.5 opacity-60" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-45">
        {SORT_OPTIONS.map((o) => (
          <DropdownMenuItem key={o.key} onClick={() => onChange(o.key)}>
            {o.label}
            {sort === o.key && <Check className="ml-auto h-3.75 w-3.75" />}
          </DropdownMenuItem>
        ))}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}

function ReportCard({
  report,
  onOpen,
  onRename,
  onDuplicate,
  onDelete,
}: {
  report: ReportSummary;
  onOpen: () => void;
  onRename: (title: string) => void;
  onDuplicate: () => void;
  onDelete: () => void;
}) {
  const [renameOpen, setRenameOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);

  const menu = (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button
          aria-label="메뉴"
          onClick={(e) => e.stopPropagation()}
          className="grid h-8 w-8 place-items-center rounded-lg text-zinc-500 transition hover:bg-zinc-100 hover:text-zinc-900"
        >
          <MoreVertical className="h-4 w-4" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" onClick={(e) => e.stopPropagation()}>
        <DropdownMenuItem onClick={onOpen}>
          <SquarePen />
          편집 열기
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => setRenameOpen(true)}>
          <Pencil />
          이름 변경
        </DropdownMenuItem>
        <DropdownMenuItem onClick={onDuplicate}>
          <Copy />
          복제
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          variant="destructive"
          onClick={() => setDeleteOpen(true)}
        >
          <Trash2 />
          삭제
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );

  const meta = (
    <div className="flex shrink-0 items-center gap-2 text-xs font-medium text-zinc-400">
      <span className="text-zinc-500">블록 {report.blockCount}개</span>
      <span className="h-0.75 w-0.75 rounded-full bg-zinc-300" />
      <span>{relTime(report.updatedAt)} 수정</span>
    </div>
  );

  return (
    <article
      onClick={onOpen}
      role="button"
      tabIndex={0}
      onKeyDown={(e) => {
        if (e.key === "Enter") onOpen();
      }}
      className="group relative flex cursor-pointer items-center overflow-hidden rounded-2xl border border-zinc-100 bg-white shadow-sm transition hover:-translate-y-0.5 hover:border-zinc-200 hover:shadow-md"
    >
      <div className="flex w-full items-center gap-3.5 p-3.5 pl-4">
        <span className="grid h-10.5 w-10.5 shrink-0 place-items-center rounded-xl bg-zinc-100 text-zinc-500">
          <FileText className="h-4.75 w-4.75" />
        </span>
        <div className="min-w-0 flex-1 text-[15px] font-bold text-zinc-900">
          <span className="block truncate">{report.title}</span>
        </div>
        {meta}
        {menu}
      </div>

      {/* 카드 onClick(=열기)이 다이얼로그 내부 클릭으로 새지 않게 차단 */}
      <div onClick={(e) => e.stopPropagation()}>
        <RenameDialog
          open={renameOpen}
          onOpenChange={setRenameOpen}
          current={report.title}
          onRename={onRename}
        />
        <Dialog open={deleteOpen} onOpenChange={setDeleteOpen}>
          <DialogContent className="sm:max-w-sm">
            <DialogHeader>
              <DialogTitle>보고서 삭제</DialogTitle>
            </DialogHeader>
            <p className="text-sm text-zinc-600">
              <b className="font-semibold text-zinc-800">{report.title}</b>{" "}
              보고서를 삭제할까요?
              <br />
              블록 {report.blockCount}개가 함께 사라집니다.
            </p>
            <DialogFooter className="flex gap-2">
              <DialogClose asChild>
                <Button variant="outline">취소</Button>
              </DialogClose>
              <DialogClose asChild>
                <Button variant="destructive" onClick={onDelete}>
                  삭제
                </Button>
              </DialogClose>
            </DialogFooter>
          </DialogContent>
        </Dialog>
      </div>
    </article>
  );
}

function CreateDialog({
  open,
  onOpenChange,
  onCreate,
  pending,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  onCreate: (title: string) => void;
  pending: boolean;
}) {
  const [title, setTitle] = useState("");
  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        onOpenChange(o);
        if (o) setTitle("");
      }}
    >
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>새 보고서</DialogTitle>
        </DialogHeader>
        <label className="text-[11.5px] font-extrabold uppercase tracking-wide text-zinc-400">
          제목
        </label>
        <input
          autoFocus
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") onCreate(title);
          }}
          placeholder="예: 9월 축제 결과 보고"
          className="w-full rounded-xl border border-zinc-200 px-3 py-2.5 text-sm text-zinc-800 outline-none transition focus:border-violet-500 focus:ring-3 focus:ring-violet-100"
        />
        <p className="text-xs text-zinc-400">
          빈 보고서로 시작합니다. 보관함에서 결과를 추가해 구성하세요.
        </p>
        <DialogFooter className="flex gap-2">
          <DialogClose asChild>
            <Button variant="outline">취소</Button>
          </DialogClose>
          <Button onClick={() => onCreate(title)} disabled={pending}>
            만들기
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function RenameDialog({
  open,
  onOpenChange,
  current,
  onRename,
}: {
  open: boolean;
  onOpenChange: (o: boolean) => void;
  current: string;
  onRename: (title: string) => void;
}) {
  const [title, setTitle] = useState(current);
  const submit = () => {
    const v = title.trim();
    if (v) onRename(v);
    onOpenChange(false);
  };
  return (
    <Dialog
      open={open}
      onOpenChange={(o) => {
        onOpenChange(o);
        if (o) setTitle(current);
      }}
    >
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>이름 변경</DialogTitle>
        </DialogHeader>
        <label className="text-[11.5px] font-extrabold uppercase tracking-wide text-zinc-400">
          보고서 제목
        </label>
        <input
          autoFocus
          value={title}
          onChange={(e) => setTitle(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") submit();
          }}
          className="w-full rounded-xl border border-zinc-200 px-3 py-2.5 text-sm text-zinc-800 outline-none transition focus:border-violet-500 focus:ring-3 focus:ring-violet-100"
        />
        <DialogFooter className="flex gap-2">
          <DialogClose asChild>
            <Button variant="outline">취소</Button>
          </DialogClose>
          <Button onClick={submit}>저장</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

function EmptyState() {
  return (
    <div className="py-[70px] text-center">
      <div className="mx-auto mb-4.5 grid h-16 w-16 place-items-center rounded-[18px] border border-zinc-100 bg-white shadow-sm">
        <Search className="h-7.5 w-7.5 text-zinc-400" />
      </div>
      <b className="block text-[17px] font-bold text-zinc-900">
        검색 결과가 없습니다
      </b>
      <span className="mt-1.5 block text-sm text-zinc-400">
        다른 검색어를 시도해 보세요.
      </span>
    </div>
  );
}
