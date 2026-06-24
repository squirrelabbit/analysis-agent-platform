import { useEffect, useRef } from "react";
import { cn } from "@/lib/utils";

// contenteditable 래퍼. React 제어 컴포넌트로 만들면 매 입력마다 re-render되어
// 커서가 튀므로, DOM은 uncontrolled로 두고 입력은 onChange로만 흘려보낸다.
// 외부에서 value가 바뀌면(예: 패널↔채팅 제목 동기화) 포커스가 없을 때만 DOM에 반영해
// 편집 중 커서를 방해하지 않는다.
interface EditableTextProps {
  value: string;
  onChange: (value: string) => void;
  className?: string;
  placeholder?: string;
  /** false(기본)면 Enter로 편집 종료(blur). true면 줄바꿈 허용. */
  multiline?: boolean;
  ariaLabel?: string;
}

export default function EditableText({
  value,
  onChange,
  className,
  placeholder,
  multiline = false,
  ariaLabel,
}: EditableTextProps) {
  const ref = useRef<HTMLDivElement>(null);

  // 마운트 시 초기 텍스트 주입(이후엔 아래 effect가 포커스 없을 때만 동기화).
  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    if (document.activeElement !== el && el.textContent !== value) {
      el.textContent = value;
    }
  }, [value]);

  return (
    <div
      ref={ref}
      role="textbox"
      aria-label={ariaLabel}
      aria-multiline={multiline}
      contentEditable
      suppressContentEditableWarning
      spellCheck={false}
      data-placeholder={placeholder}
      onInput={(e) => onChange(e.currentTarget.textContent ?? "")}
      onKeyDown={(e) => {
        if (!multiline && e.key === "Enter") {
          e.preventDefault();
          e.currentTarget.blur();
        }
      }}
      className={cn(
        "outline-none empty:before:pointer-events-none empty:before:text-zinc-400 empty:before:content-[attr(data-placeholder)]",
        className,
      )}
    />
  );
}
