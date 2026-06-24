// 분석 보고서 에디터 페이지.
// 보고서는 채팅에서 생성된 블록 구성으로 들어온다. 이 화면에서는 새 결과/차트를
// "추가"하지 않고, 기존 블록의 글(제목·해석 문구·표시 옵션)과 레이아웃 구조
// (드래그 정렬·너비·높이·삭제)만 편집한 뒤 PDF/HTML/PPTX로 내보낸다.
// 보고서 문서 API(GET 단건 → hydrate, 변경분 디바운스 PUT 자동저장)와 연동된다.
import { useEffect, useMemo, useRef, useState } from "react";
import { useParams } from "react-router-dom";
import { AlertCircle, Check, FileText, Loader2, Pencil } from "lucide-react";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import { cn } from "@/lib/utils";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { savedResultToLibraryItem } from "../models/library";
import { useSavedResults } from "../hooks/report.query";
import { useReport } from "../hooks/reportDoc.query";
import { useUpdateReport } from "../hooks/reportDoc.mutation";
import { useReportEditor } from "../hooks/useReportEditor";
import type { ReportBlock as ReportBlockModel } from "../models";
import { ReportBlock } from "../components/ReportBlock";
import { BlockPopover } from "../components/BlockPopover";
import {
  ReportToolbar,
  type ExportFormat,
} from "../components/ReportToolbar";
import {
  exportReportHTML,
  exportReportPDF,
  REPORT_EXPORT_ROOT_ID,
} from "../utils/exportReport";
import { exportReportPPTX } from "../utils/exportReportPptx";

// 드롭 삽입 지점 표시 막대(컨테이너 기준 좌표). 같은 행 사이=세로(v), 행 경계=가로(h).
type DropMarker =
  | { orient: "v"; left: number; top: number; height: number }
  | { orient: "h"; left: number; top: number; width: number };

type SaveStatus = "idle" | "dirty" | "saving" | "saved" | "error";

// 자동저장 디바운스(ms).
const AUTOSAVE_DEBOUNCE_MS = 800;

export default function ReportEditorPage() {
  const { projectId } = useProjectParams();
  const { reportId } = useParams();
  const { data: project } = useProjectDetail(projectId);
  const { state, dispatch } = useReportEditor();

  // 보고서 문서 로드 → 에디터 hydrate.
  const {
    data: report,
    isLoading: reportLoading,
    isError: reportError,
  } = useReport(projectId, reportId);
  const updateReport = useUpdateReport(projectId);

  // 자동저장 상태 + 중복 저장 방지용 마지막 저장 스냅샷.
  const [saveStatus, setSaveStatus] = useState<SaveStatus>("idle");
  const lastSavedRef = useRef<string>("");
  // 현재 상태가 어떤 보고서로 hydrate됐는지. 라우트 reportId와 일치할 때만 저장한다
  // (hydrate 전/다른 보고서 전환 순간의 stale 저장 방지).
  const hydratedIdRef = useRef<string | null>(null);
  const saveTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  // 디바운스 대기 중인 미저장 변경분. 페이지 이탈/언마운트 시 즉시 flush한다.
  const pendingRef = useRef<{ title: string; blocks: unknown[] } | null>(null);

  // 단건 로드 시 1회 hydrate. blocks는 에디터가 소유하는 contract라 그대로 캐스팅.
  useEffect(() => {
    if (!report) return;
    const blocks = (report.blocks as ReportBlockModel[]) ?? [];
    dispatch({
      type: "hydrate",
      state: {
        title: report.title,
        mode: "edit",
        selected: null,
        blocks,
        hydratedId: report.reportId,
      },
    });
    lastSavedRef.current = JSON.stringify({ title: report.title, blocks });
    hydratedIdRef.current = report.reportId;
    // 서버 로드 직후 1회 "저장됨" 동기화 — 로드 시점에만 실행돼 cascading 영향 없음.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setSaveStatus("saved");
    // report 객체 동일 id 동안은 재hydrate 안 함(편집 중 덮어쓰기 방지).
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [report?.reportId]);

  // 변경분 디바운스 자동저장(PUT 전체 교체). hydrate 직후/무변경은 skip.
  useEffect(() => {
    // state가 현재 reportId로 hydrate된 뒤에만 저장한다. hydratedIdRef(sibling effect에서
    // 세팅)는 state보다 한 렌더 앞서므로, hydrate 직후 stale DEFAULT(빈 블록)를 저장하는
    // race가 생긴다 → state와 원자적으로 갱신되는 state.hydratedId로 판단.
    if (!reportId || state.hydratedId !== reportId) return;
    const snapshot = JSON.stringify({
      title: state.title,
      blocks: state.blocks,
    });
    if (snapshot === lastSavedRef.current) {
      pendingRef.current = null;
      return;
    }
    pendingRef.current = { title: state.title, blocks: state.blocks };
    setSaveStatus("dirty");
    if (saveTimer.current) clearTimeout(saveTimer.current);
    saveTimer.current = setTimeout(() => {
      setSaveStatus("saving");
      updateReport.mutate(
        { reportId, title: state.title, blocks: state.blocks },
        {
          onSuccess: () => {
            lastSavedRef.current = snapshot;
            // 저장 직후에도 더 새로운 변경이 쌓였으면 pending 유지(이탈 시 flush 대상).
            // 방금 저장한 스냅샷과 동일할 때만 비운다.
            if (JSON.stringify(pendingRef.current) === snapshot)
              pendingRef.current = null;
            setSaveStatus("saved");
          },
          onError: () => setSaveStatus("error"),
        },
      );
    }, AUTOSAVE_DEBOUNCE_MS);
    return () => {
      if (saveTimer.current) clearTimeout(saveTimer.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [state.title, state.blocks, state.hydratedId, reportId]);

  // 페이지 이탈/언마운트 시 디바운스 대기 중이던 변경분을 즉시 저장(마지막 변경 유실 방지).
  // flush에 필요한 최신 값은 커밋 후 effect에서 ref에 담아 둔다(렌더 중 ref 갱신 금지).
  const flushDataRef = useRef({ reportId, mutate: updateReport.mutate });
  useEffect(() => {
    flushDataRef.current = { reportId, mutate: updateReport.mutate };
  });
  useEffect(
    () => () => {
      if (saveTimer.current) {
        clearTimeout(saveTimer.current);
        saveTimer.current = null;
      }
      const pending = pendingRef.current;
      const { reportId: rid, mutate } = flushDataRef.current;
      // hydrate된 보고서와 일치할 때만 저장(stale/타 보고서로의 오저장 방지).
      if (!pending || !rid || hydratedIdRef.current !== rid) return;
      pendingRef.current = null;
      mutate({ reportId: rid, title: pending.title, blocks: pending.blocks });
    },
    [],
  );

  // 보관함(saved_results) 실데이터 → 에디터 뷰모델(LibraryItem) + id 조회 맵.
  const { data: saved } = useSavedResults(projectId);
  const library = useMemo(
    () => (saved ?? []).map(savedResultToLibraryItem),
    [saved],
  );
  const libMap = useMemo(
    () => new Map(library.map((l) => [l.id, l])),
    [library],
  );
  const libById = (id: string) => libMap.get(id);

  const blocksRef = useRef<HTMLDivElement>(null);
  const gripDragUid = useRef<string | null>(null);
  const [dropMarker, setDropMarker] = useState<DropMarker | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const toastT = useRef<ReturnType<typeof setTimeout> | null>(null);
  // 드래그 중 캔버스 가장자리 자동 스크롤.
  const scrollRef = useRef<HTMLDivElement>(null);
  const autoScroll = useRef<{ vy: number; raf: number }>({ vy: 0, raf: 0 });

  const isEdit = state.mode === "edit";
  const selectedBlock = state.blocks.find((b) => b.uid === state.selected);
  const selectedLib = selectedBlock ? libById(selectedBlock.libId) : undefined;

  const showToast = (msg: string) => {
    setToast(msg);
    if (toastT.current) clearTimeout(toastT.current);
    toastT.current = setTimeout(() => setToast(null), 1900);
  };

  // 드롭 위치 계산 — 포인터에서 가장 가까운 카드 중심 기준 앞/뒤 삽입.
  // 가로(나란히)·세로(쌓기) 플로우를 모두 대응(2D 거리).
  const nearestDropIdx = (x: number, y: number): number => {
    const cards = blocksRef.current?.querySelectorAll("[data-card]");
    if (!cards || cards.length === 0) return state.blocks.length;
    let best = state.blocks.length;
    let bestDist = Infinity;
    cards.forEach((card, i) => {
      const r = card.getBoundingClientRect();
      const cx = r.left + r.width / 2;
      const cy = r.top + r.height / 2;
      const d = (x - cx) ** 2 + (y - cy) ** 2;
      if (d < bestDist) {
        bestDist = d;
        best = x < cx ? i : i + 1;
      }
    });
    return best;
  };

  // 삽입 위치 해석 → { idx, sameRow(나란히 여부), marker(표시 막대) }.
  // sameRow=true면 세로 막대(앞 블록과 같은 줄), false면 가로선(새 줄).
  const resolveDrop = (
    x: number,
    y: number,
  ): { idx: number; sameRow: boolean; marker: DropMarker | null } => {
    const idx = nearestDropIdx(x, y);
    const cont = blocksRef.current;
    const cards = cont?.querySelectorAll("[data-card]");
    if (!cont || !cards || cards.length === 0)
      return { idx, sameRow: false, marker: null };
    const cr = cont.getBoundingClientRect();
    const gap = 12;
    const left = idx > 0 ? cards[idx - 1].getBoundingClientRect() : null;
    const right = idx < cards.length ? cards[idx].getBoundingClientRect() : null;
    // 두 카드가 세로로 겹치면 같은 행으로 간주.
    const inSameRow = (a: DOMRect, b: DOMRect) =>
      a.top < b.bottom - 1 && b.top < a.bottom - 1;
    // 부분폭(전체 너비가 아님) + 오른쪽에 한 칸 들어갈 여유.
    const partial = (r: DOMRect) => r.right - r.left < cr.width - 1;
    const roomRight = (r: DOMRect) => r.right < cr.right - gap - 4;

    if (left && right && inSameRow(left, right)) {
      // 같은 행 두 카드 사이 → 세로 막대 + 나란히.
      return {
        idx,
        sameRow: true,
        marker: {
          orient: "v",
          left: (left.right + right.left) / 2 - cr.left,
          top: Math.min(left.top, right.top) - cr.top,
          height:
            Math.max(left.bottom, right.bottom) - Math.min(left.top, right.top),
        },
      };
    }
    if (
      left &&
      partial(left) &&
      roomRight(left) &&
      y >= left.top &&
      y <= left.bottom &&
      x > left.left
    ) {
      // 부분폭 블록 행의 오른쪽 빈 공간에 드롭 → 그 블록 뒤에 나란히.
      return {
        idx,
        sameRow: true,
        marker: {
          orient: "v",
          left: left.right - cr.left + gap / 2,
          top: left.top - cr.top,
          height: left.height,
        },
      };
    }
    if (right) {
      // 행 시작(위쪽 행과 경계) → 대상 행 위에 가로선 + 새 줄.
      return {
        idx,
        sameRow: false,
        marker: {
          orient: "h",
          left: 0,
          top: right.top - cr.top - gap / 2,
          width: cr.width,
        },
      };
    }
    if (left) {
      // 맨 끝. 마지막 행이 꽉 찼으면 아래 가로선(새 줄), 남는 공간 있으면 오른쪽 세로 막대(나란히).
      const rowFull = left.right >= cr.right - gap - 4;
      if (rowFull)
        return {
          idx,
          sameRow: false,
          marker: {
            orient: "h",
            left: 0,
            top: left.bottom - cr.top + gap / 2,
            width: cr.width,
          },
        };
      return {
        idx,
        sameRow: true,
        marker: {
          orient: "v",
          left: left.right - cr.left + gap / 2,
          top: left.top - cr.top,
          height: left.height,
        },
      };
    }
    return { idx, sameRow: false, marker: null };
  };

  // 스크롤 컨테이너 위/아래 가장자리에 가까우면 매 프레임 자동 스크롤.
  const tickAutoScroll = () => {
    const el = scrollRef.current;
    const { vy } = autoScroll.current;
    if (el && vy !== 0) {
      el.scrollTop += vy;
      autoScroll.current.raf = requestAnimationFrame(tickAutoScroll);
    } else {
      autoScroll.current.raf = 0;
    }
  };

  const updateAutoScroll = (clientY: number) => {
    const el = scrollRef.current;
    if (!el) return;
    const r = el.getBoundingClientRect();
    const EDGE = 64; // 가장자리 감지 영역(px)
    const MAX = 20; // 프레임당 최대 스크롤(px)
    let vy = 0;
    if (clientY < r.top + EDGE)
      vy = -MAX * Math.min(1, (r.top + EDGE - clientY) / EDGE);
    else if (clientY > r.bottom - EDGE)
      vy = MAX * Math.min(1, (clientY - (r.bottom - EDGE)) / EDGE);
    autoScroll.current.vy = vy;
    if (vy !== 0 && !autoScroll.current.raf)
      autoScroll.current.raf = requestAnimationFrame(tickAutoScroll);
  };

  const stopAutoScroll = () => {
    autoScroll.current.vy = 0;
    if (autoScroll.current.raf) {
      cancelAnimationFrame(autoScroll.current.raf);
      autoScroll.current.raf = 0;
    }
  };

  // 언마운트 시 진행 중인 자동 스크롤 정리.
  useEffect(() => stopAutoScroll, []);

  const onCanvasDragOver = (e: React.DragEvent) => {
    // 블록 재정렬(grip 드래그)만 처리한다. 결과 추가는 지원하지 않는다.
    if (gripDragUid.current == null) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    const { marker } = resolveDrop(e.clientX, e.clientY);
    setDropMarker(marker);
    updateAutoScroll(e.clientY);
  };

  const onCanvasDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const { idx, sameRow } = resolveDrop(e.clientX, e.clientY);
    if (gripDragUid.current) {
      const from = state.blocks.findIndex(
        (b) => b.uid === gripDragUid.current,
      );
      if (from >= 0)
        dispatch({ type: "moveBlock", from, to: idx, newRow: !sameRow });
      gripDragUid.current = null;
    }
    setDropMarker(null);
    stopAutoScroll();
  };

  const handleExport = (fmt: ExportFormat) => {
    if (fmt === "pptx") {
      // 블록 1개 = 슬라이드 1장, 네이티브 PPT 개체(텍스트/표/차트)로 내보낸다.
      exportReportPPTX(state.title, state.blocks, libById)
        .then((ok) =>
          showToast(
            ok ? "PPTX 파일을 다운로드했어요" : "내보낼 내용이 없습니다",
          ),
        )
        .catch(() => showToast("PPTX 내보내기에 실패했어요"));
      return;
    }
    if (fmt === "hwp") {
      showToast("한글(HWP)은 DOCX로 내보내 여는 방식으로 준비 중");
      return;
    }
    // 깔끔한 출력 위해 미리보기로 전환 후 한 틱 뒤 내보내기.
    if (state.mode !== "preview") dispatch({ type: "setMode", mode: "preview" });
    setTimeout(() => {
      if (fmt === "html") {
        showToast(
          exportReportHTML(state.title)
            ? "HTML 파일을 다운로드했어요"
            : "내보낼 내용이 없습니다",
        );
      } else {
        exportReportPDF();
      }
    }, 220);
  };

  if (reportLoading) {
    return (
      <div className="grid h-full place-items-center text-sm text-zinc-400">
        보고서를 불러오는 중…
      </div>
    );
  }
  if (reportError || !report) {
    return (
      <div className="grid h-full place-items-center text-sm text-zinc-400">
        보고서를 찾을 수 없습니다.
      </div>
    );
  }

  return (
    <div className="flex h-full">
      <div
        ref={scrollRef}
        className="min-w-0 flex-1 overflow-y-auto"
        onDragOver={(e) => {
          // 블록 재정렬 드래그 중 컨테이너 가장자리에서도 자동 스크롤 동작하도록.
          if (gripDragUid.current == null) return;
          updateAutoScroll(e.clientY);
        }}
      >
        <div className="p-8">
          <div className="mb-6">
            <Breadcrumbs
          items={[
            { label: "프로젝트", to: "/projects" },
            { label: project?.name ?? "프로젝트",  to: `/projects/${projectId}/datasets`},
            { label: "보고서", to: `/projects/${projectId}/reports` },
            { label: report?.title || "제목 없는 보고서" },
          ]}
        />
      </div>

      {/* 헤더 + 툴바 */}
      <div className="mb-6 flex flex-wrap items-end justify-between gap-4">
        <div>
          <h1 className="text-2xl font-extrabold tracking-tight text-zinc-900">
            분석 보고서
          </h1>
          <p className="mt-1.5 text-sm text-zinc-500">
            채팅에서 만든 보고서의 글과 결과 카드 배치를 편집하고 내보냅니다.
          </p>
        </div>
        <div className="flex items-center gap-3">
          <SaveIndicator status={saveStatus} />
          <ReportToolbar
            mode={state.mode}
            onMode={(mode) => dispatch({ type: "setMode", mode })}
            onExport={handleExport}
          />
        </div>
      </div>

          {/* 캔버스 */}
          <main>
          <div
            className={cn(
              "mx-auto",
              isEdit ? "max-w-205" : "max-w-190",
            )}
          >
            <div id={REPORT_EXPORT_ROOT_ID}>
              {/* 제목 입력 + 편집 가능 표시(연필). 편집 모드에서만 노출. */}
              <div className="group relative">
                <input
                  value={state.title}
                  onChange={(e) =>
                    dispatch({ type: "setTitle", title: e.target.value })
                  }
                  placeholder="보고서 제목을 입력하세요"
                  readOnly={!isEdit}
                  className={cn(
                    "w-full rounded-lg bg-transparent px-1.5 py-1 text-3xl font-extrabold tracking-tight text-zinc-900 outline-none transition placeholder:text-zinc-300 read-only:cursor-default hover:not-read-only:bg-zinc-100 focus:not-read-only:bg-white focus:not-read-only:ring-2 focus:not-read-only:ring-violet-100",
                    isEdit && "pr-10",
                  )}
                />
                {isEdit && (
                  <Pencil
                    className="pointer-events-none absolute right-3 top-1/2 h-4.5 w-4.5 -translate-y-1/2 text-zinc-300 transition-colors group-focus-within:text-violet-500"
                    strokeWidth={2.2}
                  />
                )}
              </div>
              <div className="mt-2 flex items-center gap-2.5 px-1.5 text-[13px] font-medium text-zinc-400">
                <span>작성자</span>
                <span className="h-0.75 w-0.75 rounded-full bg-zinc-300" />
                <span>2026-06-09</span>
                <span className="h-0.75 w-0.75 rounded-full bg-zinc-300" />
                <span>결과 블록 {state.blocks.length}개</span>
              </div>

              <div
                ref={blocksRef}
                className="relative mt-6 grid grid-cols-12 items-stretch gap-3"
                onDragOver={onCanvasDragOver}
                onDrop={onCanvasDrop}
              >
                {state.blocks.length === 0 ? (
                  <EmptyReport />
                ) : (
                  state.blocks.map((b, i) => {
                    const lib = libById(b.libId);
                    if (!lib) return null;
                    return (
                      <ReportBlock
                        key={b.uid}
                        block={b}
                        lib={lib}
                        index={i}
                        mode={state.mode}
                        selected={state.selected === b.uid}
                        sheetRef={blocksRef}
                        onEdit={(uid) => dispatch({ type: "select", uid })}
                        onGripDragStart={(uid) => {
                          gripDragUid.current = uid;
                        }}
                        onGripDragEnd={() => {
                          gripDragUid.current = null;
                          setDropMarker(null);
                          stopAutoScroll();
                        }}
                        onSetSpan={(uid, span) =>
                          dispatch({ type: "setSpan", uid, span })
                        }
                        onSetHeight={(uid, height) =>
                          dispatch({ type: "setHeight", uid, height })
                        }
                      />
                    );
                  })
                )}

                {/* 드롭 삽입 위치 막대(그리드 위 absolute) — 세로/가로 자동 */}
                {dropMarker &&
                  (dropMarker.orient === "v" ? (
                    <div
                      className="pointer-events-none absolute z-20 w-0.75 -translate-x-1/2 rounded-full bg-violet-600 shadow-[0_0_0_3px] shadow-violet-100"
                      style={{
                        left: dropMarker.left,
                        top: dropMarker.top,
                        height: dropMarker.height,
                      }}
                    />
                  ) : (
                    <div
                      className="pointer-events-none absolute z-20 h-0.75 -translate-y-1/2 rounded-full bg-violet-600 shadow-[0_0_0_3px] shadow-violet-100"
                      style={{
                        left: dropMarker.left,
                        top: dropMarker.top,
                        width: dropMarker.width,
                      }}
                    />
                  ))}
              </div>
            </div>
          </div>
          </main>
        </div>
      </div>

      {/* 블록 속성 팝오버 */}
      {isEdit && selectedBlock && selectedLib && (
        <BlockPopover
          block={selectedBlock}
          lib={selectedLib}
          onClose={() => dispatch({ type: "select", uid: null })}
          onSetTitle={(title) =>
            dispatch({ type: "setBlockTitle", uid: selectedBlock.uid, title })
          }
          onSetInterp={(interp) =>
            dispatch({ type: "setBlockInterp", uid: selectedBlock.uid, interp })
          }
          onToggleOpt={(key) =>
            dispatch({ type: "toggleOpt", uid: selectedBlock.uid, key })
          }
          onResetSpan={() =>
            dispatch({ type: "setSpan", uid: selectedBlock.uid, span: 6 })
          }
          onResetHeight={() =>
            dispatch({ type: "setHeight", uid: selectedBlock.uid, height: null })
          }
          onDelete={() => {
            dispatch({ type: "deleteBlock", uid: selectedBlock.uid });
            showToast("블록을 삭제했어요");
          }}
        />
      )}

      {/* 토스트 */}
      {toast && (
        <div className="fixed bottom-6 left-1/2 z-80 flex -translate-x-1/2 items-center gap-2.25 rounded-xl bg-zinc-900 px-4.5 py-2.75 text-[13.5px] font-semibold text-white shadow-2xl">
          {toast}
        </div>
      )}
    </div>
  );
}

// 자동저장 상태 칩 — 저장 중/저장됨/변경됨/실패.
function SaveIndicator({ status }: { status: SaveStatus }) {
  if (status === "idle") return null;
  if (status === "saving")
    return (
      <span className="inline-flex items-center gap-1.5 text-xs font-semibold text-zinc-400">
        <Loader2 className="h-3.5 w-3.5 animate-spin" strokeWidth={2} />
        저장 중…
      </span>
    );
  if (status === "saved")
    return (
      <span className="inline-flex items-center gap-1.5 text-xs font-semibold text-emerald-600">
        <Check className="h-3.5 w-3.5" strokeWidth={2.4} />
        저장됨
      </span>
    );
  if (status === "error")
    return (
      <span className="inline-flex items-center gap-1.5 text-xs font-semibold text-red-500">
        <AlertCircle className="h-3.5 w-3.5" strokeWidth={2} />
        저장 실패
      </span>
    );
  return (
    <span className="inline-flex items-center gap-1.5 text-xs font-semibold text-zinc-400">
      <span className="h-1.5 w-1.5 rounded-full bg-amber-400" />
      변경됨
    </span>
  );
}

function EmptyReport() {
  return (
    <div className="col-span-full py-15 text-center text-zinc-400">
      <div className="mx-auto mb-4 grid h-14 w-14 place-items-center rounded-2xl border border-zinc-100 bg-white shadow-sm">
        <FileText className="h-6.5 w-6.5 text-zinc-400" />
      </div>
      <b className="block text-base font-bold text-zinc-600">
        표시할 결과 블록이 없어요
      </b>
      <span className="text-[13.5px]">
        보고서 블록은 분석 채팅에서 결과를 저장할 때 추가됩니다
      </span>
    </div>
  );
}
