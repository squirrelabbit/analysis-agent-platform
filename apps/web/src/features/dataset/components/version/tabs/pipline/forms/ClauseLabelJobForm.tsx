import {
  ClauseLabelJobSchema,
  type ClauseLabelJobFormValues,
} from "@/features/dataset/schcema/version.schema";
import type { FormProps } from "../BuildStageCard";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { AlertCircle } from "lucide-react";
import { Checkbox } from "@/components/ui/checkbox";

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

export function ClauseLabelJobForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<ClauseLabelJobFormValues>) {
  const {
    register,
    watch,
    setValue,
    handleSubmit,
    formState: { errors },
  } = useForm<ClauseLabelJobFormValues>({
    resolver: zodResolver(ClauseLabelJobSchema),
  });

  async function handleFormSubmit(data: ClauseLabelJobFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <Field>
          <FieldLabel className="text-xs">
            프롬프트 버전<p className="text-xs text-zinc-300">(선택)</p>
          </FieldLabel>
          <Input {...register("promptVersion")} />
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
            {genuinenessOptions.map((option) => {
              const current = watch("includeGenuineness") ?? [];

              return (
                <label key={option.value} className="flex items-center gap-2">
                  <Checkbox
                    checked={current.includes(option.value)}
                    onCheckedChange={(checked) => {
                      setValue(
                        "includeGenuineness",
                        checked
                          ? [...current, option.value]
                          : current.filter((v) => v !== option.value),
                      );
                    }}
                  />

                  <span className="text-sm">{option.label}</span>
                </label>
              );
            })}

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
