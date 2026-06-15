import { useEffect } from "react";
import { Field, FieldLabel } from "@/components/ui/field";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { useLloaModelOptions } from "../../hooks/build.query";

interface LloaModelFieldProps {
  value: string;
  onChange: (next: string) => void;
  errorMessage?: string;
}

// 전처리 빌드(doc_genuineness/clause_label) 모델 선택 필드 (2026-06-12).
// LLOA_MODELS allowlist가 도착하면 default 모델을 한 번 채우고, 항목이 1개
// 이하면 선택의 의미가 없어 아무것도 렌더하지 않는다 (env default 사용).
export default function LloaModelField({
  value,
  onChange,
  errorMessage,
}: LloaModelFieldProps) {
  const { data: options, isLoading } = useLloaModelOptions();

  useEffect(() => {
    if (!options || options.length < 2 || value) return;
    const fallback = options.find((o) => o.default) ?? options[0];
    onChange(fallback.model_id);
  }, [options, value, onChange]);

  if (isLoading || !options || options.length < 2) return null;

  return (
    <Field>
      <FieldLabel className="text-xs">모델</FieldLabel>
      <Select value={value} onValueChange={onChange}>
        <SelectTrigger className="h-9 text-xs">
          <SelectValue placeholder="모델을 선택하세요" />
        </SelectTrigger>
        <SelectContent>
          {options.map((o) => (
            <SelectItem key={o.model_id} value={o.model_id} className="text-xs">
              {o.label}
              {o.default ? " (기본)" : ""}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>
      {errorMessage && <p className="text-xs text-red-500">{errorMessage}</p>}
    </Field>
  );
}
