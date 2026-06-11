// 보고서 에디터 상태 관리. useReducer + localStorage 영속화.
// NOTE: 현재는 로컬 영속(디자인 샘플). 실제 연동 시 보고서 저장/조회 API로 대체.
import { useEffect, useReducer } from "react";
import {
  DEFAULT_STATE,
  type BlockOpts,
  type ReportBlock,
  type ReportMode,
  type ReportState,
} from "../models/editor";

const LS_KEY = "report_editor";

let uidc = 100;
function newUid(): string {
  return "b" + uidc++ + "_" + Math.floor(Math.random() * 1e6).toString(36);
}

type Action =
  | { type: "setTitle"; title: string }
  | { type: "setMode"; mode: ReportMode }
  | { type: "select"; uid: string | null }
  | {
      type: "addBlock";
      libId: string;
      atIdx?: number;
      newRow?: boolean;
      // 보관함 항목에 상세표가 있는지(추가 시 detail 폴드 기본 표시 여부). 호출부에서 전달.
      hasDetail?: boolean;
    }
  | { type: "moveBlock"; from: number; to: number; newRow?: boolean }
  | { type: "deleteBlock"; uid: string }
  | { type: "setBlockTitle"; uid: string; title: string }
  | { type: "setBlockInterp"; uid: string; interp: string }
  | { type: "toggleOpt"; uid: string; key: keyof BlockOpts }
  | { type: "setSpan"; uid: string; span: number }
  | { type: "reset" };

function reducer(state: ReportState, action: Action): ReportState {
  switch (action.type) {
    case "setTitle":
      return { ...state, title: action.title };
    case "setMode":
      // 미리보기 모드에서는 선택 해제.
      return {
        ...state,
        mode: action.mode,
        selected: action.mode === "preview" ? null : state.selected,
      };
    case "select":
      return { ...state, selected: action.uid };
    case "addBlock": {
      const blk: ReportBlock = {
        uid: newUid(),
        libId: action.libId,
        title: null,
        interp: "",
        opts: { q: true, detail: !!action.hasDetail, plan: false },
        span: 12,
        newRow: action.newRow ?? true,
      };
      const blocks = [...state.blocks];
      if (action.atIdx == null || action.atIdx >= blocks.length)
        blocks.push(blk);
      else blocks.splice(action.atIdx, 0, blk);
      return { ...state, blocks, selected: blk.uid };
    }
    case "moveBlock": {
      const { from } = action;
      let { to } = action;
      // 위치 변화 없이 newRow(나란히/한 줄)만 바뀌는 경우도 처리.
      if (from === to || from === to - 1) {
        if (action.newRow == null) return state;
        return {
          ...state,
          blocks: state.blocks.map((b, i) =>
            i === from ? { ...b, newRow: action.newRow! } : b,
          ),
        };
      }
      const blocks = [...state.blocks];
      const [item] = blocks.splice(from, 1);
      if (action.newRow != null) item.newRow = action.newRow;
      if (to > from) to--;
      blocks.splice(to, 0, item);
      return { ...state, blocks };
    }
    case "deleteBlock":
      return {
        ...state,
        blocks: state.blocks.filter((b) => b.uid !== action.uid),
        selected: state.selected === action.uid ? null : state.selected,
      };
    case "setBlockTitle":
      return {
        ...state,
        blocks: state.blocks.map((b) =>
          b.uid === action.uid ? { ...b, title: action.title } : b,
        ),
      };
    case "setBlockInterp":
      return {
        ...state,
        blocks: state.blocks.map((b) =>
          b.uid === action.uid ? { ...b, interp: action.interp } : b,
        ),
      };
    case "toggleOpt":
      return {
        ...state,
        blocks: state.blocks.map((b) =>
          b.uid === action.uid
            ? { ...b, opts: { ...b.opts, [action.key]: !b.opts[action.key] } }
            : b,
        ),
      };
    case "setSpan":
      return {
        ...state,
        blocks: state.blocks.map((b) =>
          b.uid === action.uid ? { ...b, span: action.span } : b,
        ),
      };
    case "reset":
      return DEFAULT_STATE();
    default:
      return state;
  }
}

function init(): ReportState {
  try {
    const raw = localStorage.getItem(LS_KEY);
    if (raw) return JSON.parse(raw) as ReportState;
  } catch {
    /* ignore */
  }
  return DEFAULT_STATE();
}

export function useReportEditor() {
  const [state, dispatch] = useReducer(reducer, undefined, init);

  // 상태 변경마다 로컬 영속.
  useEffect(() => {
    try {
      localStorage.setItem(LS_KEY, JSON.stringify(state));
    } catch {
      /* ignore */
    }
  }, [state]);

  return { state, dispatch };
}