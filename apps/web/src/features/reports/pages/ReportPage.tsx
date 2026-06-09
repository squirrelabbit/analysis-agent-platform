// 분석 보고서 에디터 페이지.
// 좌: 저장된 결과 보관함(LIBRARY) / 우: 보고서 캔버스(블록 구성).
// 채팅에서 저장된 차트·표·원문 결과를 골라 블록으로 추가하고, 드래그 정렬·너비 조절·
// 해석 문구·표시 옵션을 편집한 뒤 PDF/HTML로 내보낸다.
// NOTE: LIBRARY와 영속(localStorage)은 디자인 샘플. 실제 결과 저장/조회·보고서 저장 API 연동 필요.
import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { FileText } from "lucide-react";
import Breadcrumbs from "@/components/common/Breadcrumbs";
import { cn } from "@/lib/utils";
import { useProjectParams } from "@/shared/hooks/useRouteParams";
import { useProjectDetail } from "@/features/projects/hooks/project.query";
import { libById } from "../models/editor";
import { useReportEditor } from "../hooks/useReportEditor";
import { ReportLibrary } from "../components/ReportLibrary";
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

export default function ReportPage() {
  const { projectId } = useProjectParams();
  const { data: project } = useProjectDetail(projectId);
  const { state, dispatch } = useReportEditor();

  const blocksRef = useRef<HTMLDivElement>(null);
  const libDragId = useRef<string | null>(null);
  const gripDragUid = useRef<string | null>(null);
  const [dropIdx, setDropIdx] = useState<number | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const toastT = useRef<ReturnType<typeof setTimeout> | null>(null);
  // 드래그 중 캔버스 가장자리 자동 스크롤.
  const scrollRef = useRef<HTMLDivElement>(null);
  const autoScroll = useRef<{ vy: number; raf: number }>({ vy: 0, raf: 0 });

  const isEdit = state.mode === "edit";
  const usedIds = useMemo(
    () => new Set(state.blocks.map((b) => b.libId)),
    [state.blocks],
  );
  const selectedBlock = state.blocks.find((b) => b.uid === state.selected);
  const selectedLib = selectedBlock ? libById(selectedBlock.libId) : undefined;

  const showToast = (msg: string) => {
    setToast(msg);
    if (toastT.current) clearTimeout(toastT.current);
    toastT.current = setTimeout(() => setToast(null), 1900);
  };

  const addBlock = (libId: string, atIdx?: number) => {
    dispatch({ type: "addBlock", libId, atIdx });
    showToast(`"${libById(libId)?.title}" 추가됨`);
  };

  // 드롭 위치 계산 — 캔버스 블록들의 중앙선 기준 가장 가까운 삽입 인덱스.
  const nearestDropIdx = (y: number): number => {
    const cards = blocksRef.current?.querySelectorAll("[data-card]");
    if (!cards) return state.blocks.length;
    for (let i = 0; i < cards.length; i++) {
      const r = cards[i].getBoundingClientRect();
      if (y < r.top + r.height / 2) return i;
    }
    return cards.length;
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
    if (libDragId.current == null && gripDragUid.current == null) return;
    e.preventDefault();
    e.dataTransfer.dropEffect = libDragId.current ? "copy" : "move";
    setDropIdx(nearestDropIdx(e.clientY));
    updateAutoScroll(e.clientY);
  };

  const onCanvasDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const idx = nearestDropIdx(e.clientY);
    if (libDragId.current) {
      addBlock(libDragId.current, idx);
      libDragId.current = null;
    } else if (gripDragUid.current) {
      const from = state.blocks.findIndex(
        (b) => b.uid === gripDragUid.current,
      );
      if (from >= 0) dispatch({ type: "moveBlock", from, to: idx });
      gripDragUid.current = null;
    }
    setDropIdx(null);
    stopAutoScroll();
  };

  const handleExport = (fmt: ExportFormat) => {
    if (fmt === "pptx") {
      showToast("PPTX는 블록=슬라이드로 매핑해 곧 지원돼요");
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

  const dropLine = (i: number) =>
    dropIdx === i ? (
      <div className="my-0.75 h-0.75 rounded-full bg-violet-600 shadow-[0_0_0_3px] shadow-violet-100" />
    ) : null;

  return (
    <div className="flex h-full">
      {isEdit && (
        <ReportLibrary
          usedIds={usedIds}
          onAdd={(libId) => addBlock(libId)}
          onDragStart={(libId) => {
            libDragId.current = libId;
          }}
          onDragEnd={() => {
            libDragId.current = null;
            setDropIdx(null);
            stopAutoScroll();
          }}
        />
      )}

      <div
        ref={scrollRef}
        className="min-w-0 flex-1 overflow-y-auto"
        onDragOver={(e) => {
          // 컨테이너 가장자리(블록 바깥 여백 포함)에서도 자동 스크롤 동작하도록.
          if (libDragId.current == null && gripDragUid.current == null) return;
          updateAutoScroll(e.clientY);
        }}
      >
        <div className="p-8">
          <div className="mb-6">
            <Breadcrumbs
          items={[
            { label: "프로젝트", to: "/projects" },
            { label: project?.name ?? "프로젝트" },
            { label: "분석 보고서" },
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
            분석 채팅에서 저장된 결과를 골라 보고서를 구성하고 내보냅니다.
          </p>
        </div>
        <ReportToolbar
          mode={state.mode}
          onMode={(mode) => dispatch({ type: "setMode", mode })}
          onReset={() => {
            if (window.confirm("편집 중인 보고서를 처음 상태로 되돌릴까요?")) {
              dispatch({ type: "reset" });
              showToast("초기화했어요");
            }
          }}
          onExport={handleExport}
        />
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
              <input
                value={state.title}
                onChange={(e) =>
                  dispatch({ type: "setTitle", title: e.target.value })
                }
                placeholder="보고서 제목을 입력하세요"
                readOnly={!isEdit}
                className="w-full rounded-lg bg-transparent px-1.5 py-1 text-3xl font-extrabold tracking-tight text-zinc-900 outline-none transition placeholder:text-zinc-300 read-only:cursor-default hover:not-read-only:bg-zinc-100 focus:not-read-only:bg-white focus:not-read-only:ring-2 focus:not-read-only:ring-violet-100"
              />
              <div className="mt-2 flex items-center gap-2.5 px-1.5 text-[13px] font-medium text-zinc-400">
                <span>작성자</span>
                <span className="h-0.75 w-0.75 rounded-full bg-zinc-300" />
                <span>2026-06-09</span>
                <span className="h-0.75 w-0.75 rounded-full bg-zinc-300" />
                <span>결과 블록 {state.blocks.length}개</span>
              </div>

              <div
                ref={blocksRef}
                className="mt-6 flex flex-col gap-3"
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
                      <Fragment key={b.uid}>
                        {dropLine(i)}
                        <ReportBlock
                          block={b}
                          lib={lib}
                          index={i}
                          mode={state.mode}
                          selected={state.selected === b.uid}
                          sheetRef={blocksRef}
                          onSelect={(uid) => dispatch({ type: "select", uid })}
                          onEdit={(uid) => dispatch({ type: "select", uid })}
                          onGripDragStart={(uid) => {
                            gripDragUid.current = uid;
                          }}
                          onGripDragEnd={() => {
                            gripDragUid.current = null;
                            setDropIdx(null);
                            stopAutoScroll();
                          }}
                          onSetWidth={(uid, width) =>
                            dispatch({ type: "setWidth", uid, width })
                          }
                        />
                      </Fragment>
                    );
                  })
                )}
                {state.blocks.length > 0 && dropLine(state.blocks.length)}

                {isEdit && (
                  <div
                    className={cn(
                      "mt-2 rounded-2xl border-2 border-dashed px-6 py-7 text-center text-sm font-semibold transition-colors",
                      dropIdx === state.blocks.length
                        ? "border-violet-500 bg-violet-50 text-violet-700"
                        : "border-zinc-300 text-zinc-400",
                    )}
                  >
                    보관함에서 결과를 끌어다 놓거나 "추가"로 블록을 삽입하세요
                  </div>
                )}
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
          onResetWidth={() =>
            dispatch({ type: "setWidth", uid: selectedBlock.uid, width: null })
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

function EmptyReport() {
  return (
    <div className="py-15 text-center text-zinc-400">
      <div className="mx-auto mb-4 grid h-14 w-14 place-items-center rounded-2xl border border-zinc-100 bg-white shadow-sm">
        <FileText className="h-6.5 w-6.5 text-zinc-400" />
      </div>
      <b className="block text-base font-bold text-zinc-600">
        아직 추가된 결과가 없어요
      </b>
      <span className="text-[13.5px]">
        왼쪽 보관함에서 결과를 추가하거나 끌어다 놓으세요
      </span>
    </div>
  );
}
