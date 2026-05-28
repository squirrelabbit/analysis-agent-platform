import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { useState } from "react";

/**
 * 키워드 입력 컴포넌트
 */
interface KeywordInputProps {
  value: string[];
  onChange: (value: string[]) => void;
  placeholder?: string;
}

export function KeywordInput({ value, onChange, placeholder }: KeywordInputProps) {
  const [input, setInput] = useState('');

  const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'Enter' && input.trim()) {
      e.preventDefault();
      const newKeyword = input.trim();
      if (!value.includes(newKeyword)) {
        onChange([...value, newKeyword]);
      }
      setInput('');
    }
  };

  const removeKeyword = (keyword: string) => {
    onChange(value.filter((k) => k !== keyword));
  };

  return (
    <div className="space-y-2">
      <Input
        value={input}
        onChange={(e) => setInput(e.target.value)}
        onKeyDown={handleKeyDown}
        placeholder={placeholder}
        className="h-10"
      />
      {value.length > 0 && (
        <div className="flex flex-wrap gap-2">
          {value.map((keyword) => (
            <Badge
              key={keyword}
              variant="secondary"
              className="cursor-pointer hover:bg-slate-300 transition-colors"
              onClick={() => removeKeyword(keyword)}
            >
              {keyword}
              <span className="ml-1">×</span>
            </Badge>
          ))}
        </div>
      )}
    </div>
  );
}