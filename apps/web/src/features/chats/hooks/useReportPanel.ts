import { useCallback, useState } from "react";
import type { ChatMessage } from "../models";

// 보고서 패널 스테이징 상태. 채팅 결과 카드를 우측 패널에 모아 제목·메모를 편집하고
// 순서를 바꾼 뒤 한 번에 보고서 문서로 만든다(시안 「분석 채팅 - 보고서 패널」).
//
// 식별자는 runId(스냅샷 단위). cardState/messages는 제거해도 유지(sticky)해서,
// 제목·메모 편집이 채팅 카드와 패널 사이를 오가도 보존되고 재추가 시 복원된다.
//
// 패널은 스레드 전환과 무관하게 유지된다 — 여러 채팅 스레드의 결과를 한 보고서에
// 모을 수 있다. 결과는 추가 시점의 스레드(runId→threadId)를 함께 보관해, 보고서
// 생성 시 각 결과를 자기 스레드 기준으로 보관함에 저장한다(백엔드 thread 검증 충족).

export interface PanelCardState {
  title: string;
  note: string;
}

export type CardType = "metric" | "quote" | "chart" | "table";

// 메인 결과 선택 규칙(metric > evidence > chart > table)에 맞춘 카드 타입.
export function cardType(msg: ChatMessage): CardType {
  if (msg.metric) return "metric";
  if (msg.evidence) return "quote";
  if (msg.chart) return "chart";
  return "table";
}

// 카드 기본 제목 — 결과에 별도 제목 필드가 없어 display/chart 제목을 쓰고, 없으면 일반 라벨.
function defaultTitle(msg: ChatMessage): string {
  return msg.display?.title ?? msg.chart?.title ?? "분석 결과";
}

export interface ReportPanelApi {
  staged: string[];
  panelOpen: boolean;
  reportTitle: string;
  count: number;
  messageOf: (runId: string) => ChatMessage | undefined;
  /** 결과가 추가될 때 보관된 출처 스레드 id(없으면 undefined → run에서 유도). */
  threadOf: (runId: string) => string | undefined;
  cardStateOf: (runId: string) => PanelCardState;
  /** 편집된 제목 또는 메시지 기준 기본 제목(스테이징 전에도 정확). */
  titleFor: (msg: ChatMessage) => string;
  isAdded: (runId: string) => boolean;
  /**
   * 결과를 패널에 추가. 새로 추가되면 true, 이미 있으면 false.
   * threadId는 출처 스레드(보고서 생성 시 보관함 저장에 사용).
   */
  add: (msg: ChatMessage, threadId?: string) => boolean;
  remove: (runId: string) => void;
  reorder: (from: number, to: number) => void;
  setTitle: (runId: string, title: string) => void;
  setNote: (runId: string, note: string) => void;
  clearAll: () => void;
  togglePanel: () => void;
  openPanel: () => void;
  closePanel: () => void;
  setReportTitle: (title: string) => void;
  reset: () => void;
}

export function useReportPanel(): ReportPanelApi {
  const [staged, setStaged] = useState<string[]>([]);
  const [panelOpen, setPanelOpen] = useState(false);
  const [reportTitle, setReportTitleState] = useState("");
  const [cards, setCards] = useState<Record<string, PanelCardState>>({});
  const [messages, setMessages] = useState<Record<string, ChatMessage>>({});
  // runId → 추가 시점의 출처 스레드 id(여러 스레드 결과 집계 지원).
  const [threads, setThreads] = useState<Record<string, string>>({});

  const messageOf = useCallback(
    (runId: string) => messages[runId],
    [messages],
  );

  const threadOf = useCallback(
    (runId: string) => threads[runId],
    [threads],
  );

  const cardStateOf = useCallback(
    (runId: string): PanelCardState =>
      cards[runId] ?? {
        title: messages[runId] ? defaultTitle(messages[runId]) : "분석 결과",
        note: "",
      },
    [cards, messages],
  );

  const titleFor = useCallback(
    (msg: ChatMessage): string => {
      const runId = msg.runId;
      if (runId && cards[runId]) return cards[runId].title;
      return defaultTitle(msg);
    },
    [cards],
  );

  const isAdded = useCallback(
    (runId: string) => staged.includes(runId),
    [staged],
  );

  const add = useCallback((msg: ChatMessage, threadId?: string): boolean => {
    const runId = msg.runId;
    if (!runId) return false;
    let added = false;
    setStaged((prev) => {
      if (prev.includes(runId)) return prev;
      added = true;
      return [...prev, runId];
    });
    // 스냅샷·기본 편집상태·출처 스레드는 항상 보존(이미 있으면 덮어쓰지 않음).
    setMessages((prev) => (prev[runId] ? prev : { ...prev, [runId]: msg }));
    setCards((prev) =>
      prev[runId]
        ? prev
        : { ...prev, [runId]: { title: defaultTitle(msg), note: "" } },
    );
    if (threadId)
      setThreads((prev) =>
        prev[runId] ? prev : { ...prev, [runId]: threadId },
      );
    setPanelOpen(true);
    return added;
  }, []);

  const remove = useCallback((runId: string) => {
    // staged에서만 제거 — cardState/messages는 유지해 편집·재추가를 보존.
    setStaged((prev) => prev.filter((id) => id !== runId));
  }, []);

  const reorder = useCallback((from: number, to: number) => {
    setStaged((prev) => {
      if (from === to || from < 0 || to < 0 || from >= prev.length || to >= prev.length)
        return prev;
      const next = [...prev];
      const [moved] = next.splice(from, 1);
      next.splice(to, 0, moved);
      return next;
    });
  }, []);

  const setTitle = useCallback((runId: string, title: string) => {
    setCards((prev) => ({
      ...prev,
      [runId]: { title, note: prev[runId]?.note ?? "" },
    }));
  }, []);

  const setNote = useCallback((runId: string, note: string) => {
    setCards((prev) => ({
      ...prev,
      [runId]: { title: prev[runId]?.title ?? "분석 결과", note },
    }));
  }, []);

  const clearAll = useCallback(() => setStaged([]), []);

  const togglePanel = useCallback(() => setPanelOpen((o) => !o), []);
  const openPanel = useCallback(() => setPanelOpen(true), []);
  const closePanel = useCallback(() => setPanelOpen(false), []);

  const setReportTitle = useCallback(
    (title: string) => setReportTitleState(title),
    [],
  );

  const reset = useCallback(() => {
    setStaged([]);
    setPanelOpen(false);
    setReportTitleState("");
    setCards({});
    setMessages({});
    setThreads({});
  }, []);

  return {
    staged,
    panelOpen,
    reportTitle,
    count: staged.length,
    messageOf,
    threadOf,
    cardStateOf,
    titleFor,
    isAdded,
    add,
    remove,
    reorder,
    setTitle,
    setNote,
    clearAll,
    togglePanel,
    openPanel,
    closePanel,
    setReportTitle,
    reset,
  };
}
