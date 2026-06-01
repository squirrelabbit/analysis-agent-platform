import type { FormProps } from "@/shared/models/common";
import { BuildCleanSchema, type BuildCleanFormValues } from "../../schemas/build.schema";
import { useFieldArray, useForm, useWatch } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/shared/utils/common";
import { Plus, X } from "lucide-react";
import { useVersion } from "../../hooks/version.query";


export default function BuildCleanForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildCleanFormValues>) {
  const { data: version } = useVersion();
  const availableColumns = version?.columns ?? [];

  const {
    handleSubmit,
    control,
    setValue,
    formState: { errors },
  } = useForm<BuildCleanFormValues>({
    resolver: zodResolver(BuildCleanSchema),
    defaultValues: { textColumns: [{ value: "" }] },
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: "textColumns",
  });

  const selected = useWatch({ control, name: "textColumns" }) ?? [];

  async function handleFormSubmit(data: BuildCleanFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <Field>
          <div className="flex items-center justify-between mb-1">
            <FieldLabel className="text-xs">
              컬럼명
              <span className="text-red-500">*</span>
            </FieldLabel>
          </div>
          <div className="flex flex-col gap-2">
            {fields.map((field, idx) => {
              const currentValue = selected[idx]?.value ?? "";
              const usedByOthers = new Set(
                selected
                  .map((s, i) => (i !== idx ? s?.value : undefined))
                  .filter((v): v is string => !!v),
              );
              const options = availableColumns.filter(
                (col) => col === currentValue || !usedByOthers.has(col),
              );
              return (
                <div key={field.id} className="flex items-center gap-2">
                  <Select
                    value={currentValue}
                    onValueChange={(v) =>
                      setValue(`textColumns.${idx}.value`, v, {
                        shouldValidate: true,
                        shouldDirty: true,
                      })
                    }
                  >
                    <SelectTrigger className="flex-1 h-9 text-xs">
                      <SelectValue placeholder="컬럼을 선택하세요" />
                    </SelectTrigger>
                    <SelectContent>
                      {options.length === 0 ? (
                        <div className="px-2 py-1.5 text-xs text-zinc-400">
                          {availableColumns.length === 0
                            ? "컬럼 정보를 불러오는 중..."
                            : "선택 가능한 컬럼이 없습니다"}
                        </div>
                      ) : (
                        options.map((col) => (
                          <SelectItem key={col} value={col} className="text-xs">
                            {col}
                          </SelectItem>
                        ))
                      )}
                    </SelectContent>
                  </Select>
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() => remove(idx)}
                    disabled={fields.length === 1}
                    className={cn(
                      "w-7 h-7 flex items-center justify-center rounded-lg border transition-colors shrink-0",
                      "border-zinc-200 text-zinc-400 hover:bg-red-50 hover:text-red-400 hover:border-red-200",

                      )}
                  >
                    <X className="w-3 h-3" />
                  </Button>
                </div>
              );
            })}

            {errors.textColumns && (
              <p className="text-xs text-red-500">
                {errors.textColumns.message ??
                  "컬럼을 올바르게 선택하세요"}
              </p>
            )}

            <Button
              type="button"
              variant="outline"
              onClick={() => append({ value: "" })}
              disabled={
                availableColumns.length > 0 &&
                fields.length >= availableColumns.length
              }
              className="flex items-center justify-center gap-1.5 w-full py-2 border border-dashed border-zinc-200 rounded-xl text-xs text-zinc-400 hover:border-indigo-300 hover:text-indigo-500 hover:bg-indigo-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              <Plus className="w-3.5 h-3.5" />
              컬럼 추가
            </Button>
          </div>
        </Field>
      </FieldGroup>
    </form>
  );
}
