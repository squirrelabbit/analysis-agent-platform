// 보고서 에디터 상태 관리. useReducer 기반.
// 서버(보고서 문서 API)가 source of truth — 단건 조회 결과를 hydrate로 주입하고,
// 변경분은 페이지에서 디바운스 자동저장(PUT)한다.
import { useReducer } from "react";
import {
  DEFAULT_STATE,
  type BlockOpts,
  type ReportMode,
  type ReportState,
} from "../models/editor";

type Action =
  | { type: "hydrate"; state: ReportState }
  | { type: "setTitle"; title: string }
  | { type: "setMode"; mode: ReportMode }
  | { type: "select"; uid: string | null }
  | { type: "moveBlock"; from: number; to: number; newRow?: boolean }
  | { type: "deleteBlock"; uid: string }
  | { type: "setBlockTitle"; uid: string; title: string }
  | { type: "setBlockInterp"; uid: string; interp: string }
  | { type: "toggleOpt"; uid: string; key: keyof BlockOpts }
  | { type: "setSpan"; uid: string; span: number }
  | { type: "setHeight"; uid: string; height: number | null }
  | { type: "reset" };

function reducer(state: ReportState, action: Action): ReportState {
  switch (action.type) {
    case "hydrate":
      // 서버 로드 결과로 상태 교체. 편집 모드로 시작, 선택 해제.
      return { ...action.state, mode: "edit", selected: null };
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
    case "setHeight":
      return {
        ...state,
        blocks: state.blocks.map((b) =>
          b.uid === action.uid ? { ...b, height: action.height } : b,
        ),
      };
    case "reset":
      return DEFAULT_STATE();
    default:
      return state;
  }
}

export function useReportEditor() {
  // 빈 상태로 시작 → 페이지가 서버 단건을 hydrate로 주입한다.
  const [state, dispatch] = useReducer(reducer, undefined, DEFAULT_STATE);
  return { state, dispatch };
}