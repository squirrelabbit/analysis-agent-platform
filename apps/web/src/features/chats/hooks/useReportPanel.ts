import { useCallback, useState } from "react";
import { createClientId } from "@/shared/utils/id";
import { projectResult } from "@/features/reports/models/result";
import { normalizeBlocks } from "@/features/reports/models/block";
import type { ReportBlock } from "@/features/reports/models";
import type { ChatMessage } from "../models";

// 보고서 패널 스테이징 상태. 채팅 결과 카드 + 기초분석 템플릿 섹션을 패널에 모아
// 제목·메모를 편집하고 순서를 바꾼 뒤, 한 번에 보고서 문서(POST /reports)로 만든다.
//
// 스테이지는 에디터와 동일한 self-contained ReportBlock 배열이다(kind: result | section).
//   - result : 채팅 분석 결과 스냅샷(runId로 중복 방지). display/plan raw 보존.
//   - section: 기초분석 템플릿 섹션(section_id로 중복 방지).
// 보고서 생성 시 이 blocks를 그대로 POST → 에디터가 normalizeBlock으로 다시 읽어 렌더한다.

function deriveResultTitle(msg: ChatMessage, question?: string): string {
  const fromDisplay = msg.rawDisplay?.title?.trim() || msg.display?.title?.trim();
  if (fromDisplay) return fromDisplay;
  const q = question?.trim();
  if (q) return q;
  const c = msg.content.trim();
  if (c) return c.length > 40 ? `${c.slice(0, 40)}…` : c;
  return "분석 결과";
}

function buildResultBlock(msg: ChatMessage, question?: string): ReportBlock {
  const pr = projectResult({ display: msg.rawDisplay, plan: msg.rawPlan });
  const hasDetail = (!!pr.metric || !!pr.evidence || !!pr.chart) && !!pr.display;
  return {
    uid: createClientId(),
    kind: "result",
    title: null,
    interp: "",
    opts: { q: !!question?.trim(), detail: hasDetail, plan: false },
    span: 12,
    height: null,
    newRow: true,
    result: {
      runId: msg.runId,
      question: question?.trim() ?? "",
      assistantContent: msg.content,
      defaultTitle: deriveResultTitle(msg, question),
      display: msg.rawDisplay,
      plan: msg.rawPlan,
    },
  };
}

export interface ReportPanelApi {
  staged: ReportBlock[];
  panelOpen: boolean;
  reportTitle: string;
  count: number;
  /** 펼쳐진 카드 uid 집합. 여러 카드를 동시에 펼칠 수 있다(다중 열기). */
  expandedIds: Set<string>;
  hasSections: boolean;
  /** 불러온/저장된 보고서 id(null=아직 저장 안 된 새 보고서). 저장 시 PUT(갱신) vs POST(생성) 결정. */
  loadedReportId: string | null;
  /** 대상 보고서가 정해졌는지(새 보고서 시작 또는 기존 불러오기). false면 결과 추가 전 선택 필요. */
  started: boolean;
  /** 마지막 저장 이후 스테이지에 변경(추가/삭제/순서/제목/메모)이 있는지. 미저장 가드 판단용. */
  dirty: boolean;
  /** 기존 보고서를 스테이지로 불러온다(이어서 편집·추가 → 저장 시 갱신). */
  loadReport: (reportId: string, title: string, blocks: unknown[]) => void;
  /** 새 보고서로 시작(스테이지 비움). */
  startNew: () => void;
  /** runId가 이미 스테이지에 추가됐는지(중복 추가 방지/카드 상태). */
  isAddedRun: (runId: string) => boolean;
  /** 채팅 결과를 스테이지에 추가. 새로 추가되면 true, 이미 있으면 false. */
  addResult: (msg: ChatMessage, question?: string) => boolean;
  /** 기초분석 템플릿 섹션 블록을 추가(section_id 중복은 건너뜀). 추가된 개수 반환. */
  addSections: (blocks: ReportBlock[]) => number;
  remove: (uid: string) => void;
  reorder: (from: number, to: number) => void;
  setTitle: (uid: string, title: string) => void;
  setNote: (uid: string, note: string) => void;
  toggleExpand: (uid: string) => void;
  /** 저장만(이동 없이) 성공한 뒤 호출 — 이후 재저장이 갱신(PUT)이 되도록 보고서 id를
   *  기억하고, dirty를 내려 미저장 가드를 끈다. staged/펼침 상태는 유지. */
  markSaved: (reportId: string) => void;
  clearAll: () => void;
  togglePanel: () => void;
  openPanel: () => void;
  closePanel: () => void;
  setReportTitle: (title: string) => void;
  reset: () => void;
}

export function useReportPanel(): ReportPanelApi {
  const [staged, setStaged] = useState<ReportBlock[]>([]);
  const [panelOpen, setPanelOpen] = useState(false);
  const [reportTitle, setReportTitleState] = useState("");
  const [expandedIds, setExpandedIds] = useState<Set<string>>(new Set());
  const [loadedReportId, setLoadedReportId] = useState<string | null>(null);
  const [started, setStarted] = useState(false);
  const [dirty, setDirty] = useState(false);

  const loadReport = useCallback(
    (reportId: string, title: string, blocks: unknown[]) => {
      setStaged(normalizeBlocks(blocks));
      setLoadedReportId(reportId);
      setReportTitleState(title);
      setExpandedIds(new Set());
      setStarted(true);
      setDirty(false);
      setPanelOpen(true);
    },
    [],
  );

  const startNew = useCallback(() => {
    setStaged([]);
    setLoadedReportId(null);
    setReportTitleState("");
    setExpandedIds(new Set());
    setStarted(true);
    setDirty(false);
    setPanelOpen(true);
  }, []);

  const isAddedRun = useCallback(
    (runId: string) => staged.some((b) => b.result?.runId === runId),
    [staged],
  );

  const addResult = useCallback((msg: ChatMessage, question?: string): boolean => {
    if (!msg.runId) return false;
    let added = false;
    setStaged((prev) => {
      if (prev.some((b) => b.result?.runId === msg.runId)) return prev;
      added = true;
      const block = buildResultBlock(msg, question);
      // 새로 추가된 카드는 펼쳐 두되, 기존에 펼친 카드는 그대로 둔다(다중 열기).
      setExpandedIds((cur) => new Set(cur).add(block.uid));
      setDirty(true);
      return [...prev, block];
    });
    setPanelOpen(true);
    return added;
  }, []);

  const addSections = useCallback((blocks: ReportBlock[]): number => {
    let count = 0;
    setStaged((prev) => {
      const existing = new Set(
        prev
          .filter((b) => b.kind === "section")
          .map((b) => b.section?.sectionId),
      );
      const fresh = blocks.filter(
        (b) => b.kind === "section" && !existing.has(b.section?.sectionId),
      );
      count = fresh.length;
      if (!fresh.length) return prev;
      setDirty(true);
      return [...prev, ...fresh];
    });
    setPanelOpen(true);
    return count;
  }, []);

  const remove = useCallback((uid: string) => {
    setStaged((prev) => prev.filter((b) => b.uid !== uid));
    setExpandedIds((cur) => {
      if (!cur.has(uid)) return cur;
      const next = new Set(cur);
      next.delete(uid);
      return next;
    });
    setDirty(true);
  }, []);

  const reorder = useCallback((from: number, to: number) => {
    setStaged((prev) => {
      if (
        from === to ||
        from < 0 ||
        to < 0 ||
        from >= prev.length ||
        to >= prev.length
      )
        return prev;
      const next = [...prev];
      const [moved] = next.splice(from, 1);
      next.splice(to, 0, moved);
      return next;
    });
    setDirty(true);
  }, []);

  const setTitle = useCallback((uid: string, title: string) => {
    setStaged((prev) =>
      prev.map((b) => (b.uid === uid ? { ...b, title } : b)),
    );
    setDirty(true);
  }, []);

  const setNote = useCallback((uid: string, note: string) => {
    setStaged((prev) =>
      prev.map((b) => (b.uid === uid ? { ...b, interp: note } : b)),
    );
    setDirty(true);
  }, []);

  const toggleExpand = useCallback(
    (uid: string) =>
      setExpandedIds((cur) => {
        const next = new Set(cur);
        if (next.has(uid)) next.delete(uid);
        else next.add(uid);
        return next;
      }),
    [],
  );

  const markSaved = useCallback((reportId: string) => {
    setLoadedReportId(reportId);
    setStarted(true);
    setDirty(false);
  }, []);

  const clearAll = useCallback(() => {
    setStaged([]);
    setDirty(true);
  }, []);
  const togglePanel = useCallback(() => setPanelOpen((o) => !o), []);
  const openPanel = useCallback(() => setPanelOpen(true), []);
  const closePanel = useCallback(() => setPanelOpen(false), []);
  const setReportTitle = useCallback((title: string) => {
    setReportTitleState(title);
    setDirty(true);
  }, []);

  const reset = useCallback(() => {
    setStaged([]);
    setPanelOpen(false);
    setReportTitleState("");
    setExpandedIds(new Set());
    setLoadedReportId(null);
    setStarted(false);
    setDirty(false);
  }, []);

  return {
    staged,
    panelOpen,
    reportTitle,
    count: staged.length,
    expandedIds,
    hasSections: staged.some((b) => b.kind === "section"),
    loadedReportId,
    started,
    dirty,
    loadReport,
    startNew,
    isAddedRun,
    addResult,
    addSections,
    remove,
    reorder,
    setTitle,
    setNote,
    toggleExpand,
    markSaved,
    clearAll,
    togglePanel,
    openPanel,
    closePanel,
    setReportTitle,
    reset,
  };
}
