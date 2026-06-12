import type { FormProps } from "@/shared/models/common";
import { BuildClauseSchema, type BuildClauseFormValues } from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Checkbox } from "@/components/ui/checkbox";
import { AlertCircle } from "lucide-react";
import PromptVersionField from "@/features/prompts/components/PromptVersionField";
import LloaModelField from "./LloaModelField";

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
    setValue,
    control,
    handleSubmit,
    formState: { errors },
  } = useForm<BuildClauseFormValues>({
    resolver: zodResolver(BuildClauseSchema),
  });

  const promptVersion = useWatch({ control, name: "promptVersion" }) ?? "";
  const currentInclude = useWatch({ control, name: "includeGenuineness" }) ?? [];
  const modelId = useWatch({ control, name: "modelId" }) ?? "";

  async function handleFormSubmit(data: BuildClauseFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <p className="text-sm text-zinc-500">
          문서를 절 단위로 나누고 각 절의 감성과 aspect(주제)를 라벨링합니다.
          진성 분석 완료 후 실행할 수 있습니다.
        </p>
        <PromptVersionField
          task="clause_label"
          value={promptVersion}
          onChange={(v) =>
            setValue("promptVersion", v, { shouldValidate: true })
          }
          errorMessage={errors.promptVersion?.message}
        />
        <LloaModelField
          value={modelId}
          onChange={(v) => setValue("modelId", v, { shouldValidate: true })}
          errorMessage={errors.modelId?.message}
        />
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
