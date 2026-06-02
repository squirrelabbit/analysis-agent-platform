import { useEffect } from "react";
import { Field, FieldLabel } from "@/components/ui/field";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { usePromptOptions } from "../hooks/prompt.query";
import type { PromptOptionsTask } from "../models";

interface PromptVersionFieldProps {
  task: PromptOptionsTask;
  value: string;
  onChange: (next: string) => void;
  errorMessage?: string;
}

// 빌드 단계별 프롬프트 버전 선택 필드. 카탈로그가 도착하면 default를
// 한 번 채우고, 버전이 1개면 굳이 Select를 띄우지 않고 readonly로 표시한다.
export default function PromptVersionField({
  task,
  value,
  onChange,
  errorMessage,
}: PromptVersionFieldProps) {
  const { data: options, isLoading, isError } = usePromptOptions(task);

  useEffect(() => {
    if (!options || value) return;
    onChange(options.default);
  }, [options, value, onChange]);

  const versions = options?.versions ?? [];
  const single = versions.length === 1;
  const selectedLabel =
    versions.find((v) => v.version === value)?.label ?? value;

  return (
    <Field>
      <FieldLabel className="text-xs">프롬프트 버전</FieldLabel>

      {single ? (
        <div className="h-9 flex items-center px-3 text-xs text-zinc-700 border border-zinc-200 rounded-md bg-zinc-50">
          {selectedLabel}
        </div>
      ) : (
        <Select
          value={value}
          onValueChange={onChange}
          disabled={isLoading || isError || versions.length === 0}
        >
          <SelectTrigger className="h-9 text-xs">
            <SelectValue
              placeholder={
                isLoading
                  ? "버전 목록을 불러오는 중..."
                  : isError
                    ? "버전 목록을 불러오지 못했습니다"
                    : "버전을 선택하세요"
              }
            />
          </SelectTrigger>
          <SelectContent>
            {versions.map((v) => (
              <SelectItem key={v.version} value={v.version} className="text-xs">
                {v.label}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      )}

      {isError && (
        <p className="text-xs text-red-500">
          프롬프트 버전 목록을 불러오지 못해 실행할 수 없습니다.
        </p>
      )}
      {errorMessage && (
        <p className="text-xs text-red-500">{errorMessage}</p>
      )}
    </Field>
  );
}
