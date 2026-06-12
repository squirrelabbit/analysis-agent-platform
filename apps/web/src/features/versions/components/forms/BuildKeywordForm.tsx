import type { FormProps } from "@/shared/models/common";
import {
  BuildKeywordSchema,
  type BuildKeywordFormValues,
} from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { ToggleGroup, ToggleGroupItem } from "@/components/ui/toggle-group";
import { Input } from "@/components/ui/input";

// 의미 있는 범위가 좁아(1~3) 자유 입력 대신 프리셋으로 효과를 설명한다.
const MIN_LEN_PRESETS = [
  { value: 1, label: "1글자 이상" },
  { value: 2, label: "2글자 이상" },
  { value: 3, label: "3글자 이상" },
];

export default function BuildKeywordForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildKeywordFormValues>) {
  const { handleSubmit, setValue, control } = useForm<BuildKeywordFormValues>({
    resolver: zodResolver(BuildKeywordSchema),
    // worker default와 동일한 2를 미리 선택해 실제 적용값을 그대로 노출한다.
    defaultValues: { keywordMinLen: 2 },
  });

  // 프리셋·직접 입력이 공유하는 값. 미선택(undefined)이면 전송 안 함 → worker default(2).
  const minLen = useWatch({ control, name: "keywordMinLen" });

  const setMinLen = (n: number | undefined) =>
    // 1 미만/비정상 값은 미선택으로 처리(백엔드도 0이면 default 2).
    setValue("keywordMinLen", n != null && n >= 1 ? n : undefined, {
      shouldValidate: true,
    });

  async function handleFormSubmit(data: BuildKeywordFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <p className="text-sm text-zinc-500">
          절 라벨링 결과에서 키워드를 자동 추출합니다. 추가 설정 없이 바로
          실행할 수 있습니다.
        </p>

        <Field>
          <FieldLabel className="text-xs">
            키워드 최소 길이
            <span className="ml-1 text-xs text-zinc-300">(선택)</span>
          </FieldLabel>
          <div className="flex flex-wrap items-center gap-2">
            <ToggleGroup
              type="single"
              spacing={1}
              // 직접 입력한 값이 프리셋과 다르면 어떤 항목도 선택되지 않는다.
              value={minLen != null ? String(minLen) : ""}
              onValueChange={(v) => setMinLen(v ? Number(v) : undefined)}
            >
              {MIN_LEN_PRESETS.map((p) => (
                <ToggleGroupItem
                  key={p.value}
                  value={String(p.value)}
                  variant="outline"
                  className="px-3 text-xs data-[state=on]:border-violet-600 data-[state=on]:bg-violet-600 data-[state=on]:text-white"
                >
                  {p.label}
                </ToggleGroupItem>
              ))}
            </ToggleGroup>

            <span className="text-[11px] text-zinc-400">또는</span>
            <div className="flex items-center gap-1">
              <Input
                type="number"
                min={1}
                value={minLen ?? ""}
                onChange={(e) => {
                  const n = parseInt(e.target.value, 10);
                  setMinLen(Number.isFinite(n) ? n : undefined);
                }}
                placeholder="직접"
                className="h-8 w-16 text-xs"
              />
              <span className="text-xs text-zinc-500">글자</span>
            </div>
          </div>
          <p className="text-[11px] text-zinc-400">
            설정한 글자 수 미만의 짧은 키워드를 제외합니다. 기본값은 2글자입니다.
          </p>
        </Field>
      </FieldGroup>
    </form>
  );
}
