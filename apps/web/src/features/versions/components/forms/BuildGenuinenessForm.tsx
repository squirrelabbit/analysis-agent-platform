import { useEffect } from "react";
import type { FormProps } from "@/shared/models/common";
import { BuildGenuinenessSchema, type BuildGenuinenessFormValues } from "../../schemas/build.schema";
import { useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { usePromptOptions } from "@/features/prompts/hooks/prompt.query";


export default function BuildGenuinenessForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildGenuinenessFormValues>) {
  const {
    data: promptOptions,
    isLoading: promptLoading,
    isError: promptError,
  } = usePromptOptions("doc_genuineness");

  const {
    handleSubmit,
    setValue,
    control,
    formState: { errors },
  } = useForm<BuildGenuinenessFormValues>({
    resolver: zodResolver(BuildGenuinenessSchema),
  });

  const currentValue = useWatch({ control, name: "promptVersion" }) ?? "";

  // 서버 default를 폼 기본값으로 — 카탈로그 도착 후 비어있을 때 한 번 채움.
  useEffect(() => {
    if (!promptOptions || currentValue) return;
    setValue("promptVersion", promptOptions.default, { shouldValidate: true });
  }, [promptOptions, currentValue, setValue]);

  async function handleFormSubmit(data: BuildGenuinenessFormValues) {
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
            value={currentValue}
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
      </FieldGroup>
    </form>
  );
}
