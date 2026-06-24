import { createContext, useContext } from "react";
import type { ChatThread } from "../models";

// 대화 이력은 시안(「분석 채팅 - 보고서 패널」)대로 공용 메인 사이드바의 "채팅" 항목
// 아래에 그려진다. 사이드바(Sidebar, ProjectLayout이 렌더)와 채팅 본문(ChatPage,
// Outlet 자식)은 형제라 직접 상태를 공유할 수 없으므로, 두 화면이 합의해야 하는
// "내비게이션 슬라이스"(현재 데이터셋·선택 스레드·전송중 여부 + 목록/삭제)만
// ProjectLayout 레벨 컨텍스트로 끌어올린다. pendingTurn·스크롤·입력·보고서 패널 같은
// 채팅 본문 전용 상태는 ChatPage에 그대로 둔다.
//
// (컨텍스트/훅은 JSX가 없는 이 모듈에, Provider 컴포넌트는 ChatNavProvider.tsx에 둔다 —
//  fast-refresh가 컴포넌트 전용 파일에서만 동작하므로 분리.)
export interface ChatNavValue {
  datasetId: string;
  /** datasetId 미선택 시 첫 데이터셋으로 폴백한 실제 사용 값. */
  activeDatasetId: string;
  /** 데이터셋 전환 — 스레드 선택도 함께 해제(스레드는 데이터셋 스코프). */
  setDatasetId: (id: string) => void;
  threadId: string | null;
  /** 사이드바에서 과거 대화 클릭. 전송 중이거나 같은 스레드면 무시. */
  selectThread: (id: string) => void;
  /** 새 대화 시작(threadId 해제). */
  newThread: () => void;
  /** 전송 직후 새 서버 threadId로 승격할 때 ChatPage가 직접 쓰는 무가드 setter. */
  setThreadId: (id: string | null) => void;
  /** 대화 삭제 — 활성 스레드를 지우면 threadId도 해제. */
  deleteThread: (id: string) => Promise<void>;
  threads: ChatThread[];
  threadsLoading: boolean;
  deletingThreadId: string | null;
  isComposing: boolean;
  setComposing: (v: boolean) => void;
  /** 현재 라우트가 채팅 화면인지 — 사이드바 이력 노출/목록 fetch 게이트. */
  isChatRoute: boolean;
}

export const ChatNavContext = createContext<ChatNavValue | null>(null);

export function useChatNav(): ChatNavValue {
  const ctx = useContext(ChatNavContext);
  if (!ctx) {
    throw new Error("useChatNav는 ChatNavProvider 안에서만 사용할 수 있습니다.");
  }
  return ctx;
}
