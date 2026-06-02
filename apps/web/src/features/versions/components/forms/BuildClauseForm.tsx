import { useEffect } from "react";
import type { FormProps } from "@/shared/models/common";
import { BuildClauseSchema, type BuildClauseFormValues } from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Checkbox } from "@/components/ui/checkbox";
import { AlertCircle } from "lucide-react";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { usePromptOptions } from "@/features/prompts/hooks/prompt.query";

const genuinenessOptions = [
  {
    label: "실제 리뷰",
    value: "genuine_review",
  },
  {
    label: "혼합",
    value: "mixed",
  },
];

export function BuildClauseForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildClauseFormValues>) {
  const {
    data: promptOptions,
    isLoading: promptLoading,
    isError: promptError,
  } = usePromptOptions("clause_label");

  const {
    setValue,
    control,
    handleSubmit,
    formState: { errors },
  } = useForm<BuildClauseFormValues>({
    resolver: zodResolver(BuildClauseSchema),
  });

  const currentPromptVersion =
    useWatch({ control, name: "promptVersion" }) ?? "";
  const currentInclude = useWatch({ control, name: "includeGenuineness" }) ?? [];

  // 서버 default를 폼 기본값으로 — 카탈로그 도착 후 비어있을 때 한 번 채움.
  useEffect(() => {
    if (!promptOptions || currentPromptVersion) return;
    setValue("promptVersion", promptOptions.default, { shouldValidate: true });
  }, [promptOptions, currentPromptVersion, setValue]);

  async function handleFormSubmit(data: BuildClauseFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <Field>
          <FieldLabel className="text-xs">
            프롬프트 버전
          </FieldLabel>
          <Select
            value={currentPromptVersion}
            onValueChange={(v) =>
              setValue("promptVersion", v, { shouldValidate: true })
            }
            disabled={promptLoading || promptError || !promptOptions?.versions.length}
          >
            <SelectTrigger className="h-9 text-xs">
              <SelectValue
                placeholder={
                  promptLoading
                    ? "버전 목록을 불러오는 중..."
                    : promptError
                      ? "버전 목록을 불러오지 못했습니다"
                      : "버전을 선택하세요"
                }
              />
            </SelectTrigger>
            <SelectContent>
              {promptOptions?.versions.map((v) => (
                <SelectItem key={v.version} value={v.version} className="text-xs">
                  {v.label}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          {promptError && (
            <p className="text-xs text-red-500">
              프롬프트 버전 목록을 불러오지 못해 실행할 수 없습니다.
            </p>
          )}
          {errors.promptVersion && (
            <p className="text-xs text-red-500">
              {errors.promptVersion.message}
            </p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            포함할 리뷰 유형
            <span className="ml-1 text-xs text-zinc-300">(선택)</span>
          </FieldLabel>

          <div className="space-y-2 rounded-lg border p-3">
            {genuinenessOptions.map((option) => (
              <label key={option.value} className="flex items-center gap-2">
                <Checkbox
                  checked={currentInclude.includes(option.value)}
                  onCheckedChange={(checked) => {
                    setValue(
                      "includeGenuineness",
                      checked
                        ? [...currentInclude, option.value]
                        : currentInclude.filter((v) => v !== option.value),
                    );
                  }}
                />

                <span className="text-sm">{option.label}</span>
              </label>
            ))}

            <div className="flex items-center gap-1 pt-1 text-[11px] text-amber-600">
              <AlertCircle className="h-3 w-3 shrink-0" />
              진성 분류 실행 후 사용 가능합니다
            </div>
          </div>
        </Field>
      </FieldGroup>
    </form>
  );
}
