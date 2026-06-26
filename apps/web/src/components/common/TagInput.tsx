import { useState, useRef, type KeyboardEvent,  } from "react";
import { X } from "lucide-react";
import { cn } from "@/lib/utils";

interface TagInputProps {
  tags: string[];
  onChange: (tags: string[]) => void;
  placeholder?: string;
  variant?: "blue" | "amber";
  className?: string;
}

export function TagInput({
  tags,
  onChange,
  placeholder = "입력 후 Enter",
  variant = "blue",
  className,
}: TagInputProps) {
  const [inputVal, setInputVal] = useState("");
  const inputRef = useRef<HTMLInputElement>(null);

  const tagStyle =
    variant === "blue"
      ? "bg-[#E6F1FB] text-[#0C447C] border-[#B5D4F4]"
      : "bg-[#FAEEDA] text-[#633806] border-[#FAC775]";

  const removeStyle =
    variant === "blue"
      ? "bg-[rgba(24,95,165,0.15)] text-[#0C447C] hover:bg-[rgba(24,95,165,0.3)]"
      : "bg-[rgba(186,117,23,0.15)] text-[#633806] hover:bg-[rgba(186,117,23,0.3)]";

  const addTag = (val: string) => {
    const trimmed = val.trim().replace(/,$/, "");
    if (trimmed && !tags.includes(trimmed)) {
      onChange([...tags, trimmed]);
    }
    setInputVal("");
  };

  const removeTag = (index: number) => {
    onChange(tags.filter((_, i) => i !== index));
  };

  const handleKeyDown = (e: KeyboardEvent<HTMLInputElement>) => {
    // 한글 등 IME 조합 중 Enter/쉼표는 무시 — 마지막 음절 중복 태그 방지.
    if (e.nativeEvent.isComposing) return;
    if (e.key === "Enter" || e.key === ",") {
      e.preventDefault();
      addTag(inputVal);
    }
    if (e.key === "Backspace" && inputVal === "" && tags.length > 0) {
      onChange(tags.slice(0, -1));
    }
  };

  return (
    <div
      className={cn(
        "flex flex-wrap gap-1.5 px-2.5 py-2 min-h-9 border border-zinc-200 rounded-lg cursor-text transition-colors",
        "focus-within:border-blue-500 focus-within:ring-2 focus-within:ring-blue-500/10",
        "bg-white",
        className
      )}
      onClick={() => inputRef.current?.focus()}
    >
      {tags.map((tag, i) => (
        <span
          key={i}
          className={cn(
            "inline-flex items-center gap-1 pl-2 pr-1 py-0.5 rounded-full text-xs font-medium border",
            tagStyle
          )}
        >
          {tag}
          <button
            type="button"
            onClick={(e) => {
              e.stopPropagation();
              removeTag(i);
            }}
            className={cn(
              "w-3.5 h-3.5 rounded-full inline-flex items-center justify-center transition-colors shrink-0",
              removeStyle
            )}
            aria-label={`${tag} 삭제`}
          >
            <X className="w-2.5 h-2.5" />
          </button>
        </span>
      ))}
      <input
        ref={inputRef}
        type="text"
        value={inputVal}
        onChange={(e) => setInputVal(e.target.value)}
        onKeyDown={handleKeyDown}
        onBlur={() => inputVal && addTag(inputVal)}
        placeholder={tags.length === 0 ? placeholder : ""}
        className="flex-1 min-w-20 text-xs bg-transparent outline-none text-zinc-800 placeholder:text-zinc-400 py-0.5"
      />
    </div>
  );
}
