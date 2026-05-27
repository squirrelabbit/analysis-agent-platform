import type { FormProps } from "@/shared/model/common";
import { BuildCleanSchema, type BuildCleanFormValues } from "../../schemas/build.schema";
import { useFieldArray, useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { cn } from "@/shared/utils/common";
import { Plus, X } from "lucide-react";


export default function BuildCleanForm({
  formId,
  onSubmit,
  onSuccess,
}: FormProps<BuildCleanFormValues>) {
  const {
    register,
    handleSubmit,
    control,
    formState: { errors },
  } = useForm<BuildCleanFormValues>({
    resolver: zodResolver(BuildCleanSchema),
    // defaultValues: {
    //   cleanOptions: {
    //     removeEnglish: false,
    //     removeNumbers: false,
    //     removeSpecial: false,
    //     removeMonosyllables: false,
    //   },
    // },
  });

  const { fields, append, remove } = useFieldArray({
    control,
    name: "textColumns",
  });

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
            {fields.map((field, idx) => (
              <div key={field.id} className="flex items-center gap-2">
                <Input
                  {...register(`textColumns.${idx}.value` as const)}
                  placeholder="예: 제목"
                />
                <Button
                  variant="outline"
                  onClick={() => remove(idx)}
                  className={cn(
                    "w-7 h-7 flex items-center justify-center rounded-lg border transition-colors shrink-0",
                    "border-zinc-200 text-zinc-400 hover:bg-red-50 hover:text-red-400 hover:border-red-200",

                    )}
                >
                  <X className="w-3 h-3" />
                </Button>
              </div>
            ))}

            {errors.textColumns && (
              <p className="text-xs text-red-500">
                {errors.textColumns.message ??
                  "키워드 정보를 올바르게 입력하세요"}
              </p>
            )}

            <Button
              variant="outline"
              onClick={() => append({ value: "" })}
              className="flex items-center justify-center gap-1.5 w-full py-2 border border-dashed border-zinc-200 rounded-xl text-xs text-zinc-400 hover:border-indigo-300 hover:text-indigo-500 hover:bg-indigo-50 transition-colors"
            >
              <Plus className="w-3.5 h-3.5" />
              키워드 추가
            </Button>
          </div>
        </Field>
        {/* <Field>
          <FieldLabel className="text-xs">
            전처리 옵션<p className="text-xs text-zinc-300">(선택)</p>
          </FieldLabel>
          <Controller
            control={control}
            name="cleanOptions"
            render={({ field }) => (
              <CleanOptionsAccordion
                value={field.value}
                onChange={field.onChange}
              />
            )}
          />
        </Field> */}
      </FieldGroup>
    </form>
  );
}
