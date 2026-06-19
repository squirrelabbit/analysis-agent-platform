import { useCallback, useState } from "react";
import type { ChatMessage } from "../models";

// 보고서 패널 스테이징 상태. 채팅 결과 카드를 우측 패널에 모아 제목·메모를 편집하고
// 순서를 바꾼 뒤 한 번에 보고서 문서로 만든다(시안 「분석 채팅 - 보고서 패널」).
//
// 식별자는 runId(스냅샷 단위). cardState/messages는 제거해도 유지(sticky)해서,
// 제목·메모 편집이 채팅 카드와 패널 사이를 오가도 보존되고 재추가 시 복원된다.

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
  cardStateOf: (runId: string) => PanelCardState;
  /** 편집된 제목 또는 메시지 기준 기본 제목(스테이징 전에도 정확). */
  titleFor: (msg: ChatMessage) => string;
  isAdded: (runId: string) => boolean;
  /** 결과를 패널에 추가. 새로 추가되면 true, 이미 있으면 false. */
  add: (msg: ChatMessage) => boolean;
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

  const messageOf = useCallback(
    (runId: string) => messages[runId],
    [messages],
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

  const add = useCallback((msg: ChatMessage): boolean => {
    const runId = msg.runId;
    if (!runId) return false;
    let added = false;
    setStaged((prev) => {
      if (prev.includes(runId)) return prev;
      added = true;
      return [...prev, runId];
    });
    // 스냅샷·기본 편집상태는 항상 보존(이미 있으면 덮어쓰지 않음).
    setMessages((prev) => (prev[runId] ? prev : { ...prev, [runId]: msg }));
    setCards((prev) =>
      prev[runId]
        ? prev
        : { ...prev, [runId]: { title: defaultTitle(msg), note: "" } },
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
  }, []);

  return {
    staged,
    panelOpen,
    reportTitle,
    count: staged.length,
    messageOf,
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
