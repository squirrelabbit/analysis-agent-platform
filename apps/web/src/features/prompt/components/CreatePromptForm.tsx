import { Controller, useForm } from "react-hook-form";
import { promptSchema, type PromptFormValues } from "../schema/prompt.schema";
import { zodResolver } from "@hookform/resolvers/zod";
import { Field, FieldGroup, FieldLabel, FieldTitle } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { FileText, Layers } from "lucide-react";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

interface Props {
  formId: string;
  onSubmit: (data: PromptFormValues) => Promise<void>;
  onSuccess: () => void;
}

const MODE_OPTIONS = [
  { value: "single", label: "단일", icon: FileText },
  { value: "batch", label: "전체", icon: Layers },
];

export default function CreatePromptForm({
  formId,
  onSubmit,
  onSuccess,
}: Props) {
  const {
    register,
    handleSubmit,
    control,
    formState: { errors },
  } = useForm<PromptFormValues>({
    resolver: zodResolver(promptSchema),
    defaultValues: { type: "prepare", mode: 'single' },
  });

  async function handleFormSubmit(data: PromptFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup>
        <Field>
          <FieldLabel className="text-xs">
            프롬프트 버전 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input {...register("version")} placeholder="project-prepare-v1" />
          {errors.version && (
            <p className="text-xs text-red-500">{errors.version.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            처리 유형 <span className="text-red-500">*</span>
          </FieldLabel>

          <Controller
            name="type"
            control={control}
            render={({ field }) => (
              <Select value={field.value} onValueChange={field.onChange}>
                <SelectTrigger className="h-9 text-xs">
                  <SelectValue placeholder="선택하세요" />
                </SelectTrigger>

                <SelectContent>
                  <SelectItem value="prepare">데이터 전처리</SelectItem>
                  <SelectItem value="sentiment">감정 분석</SelectItem>
                </SelectContent>
              </Select>
            )}
          />

          {errors.type && (
            <p className="text-xs text-red-500">{errors.type.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            처리 범위 <span className="text-red-500">*</span>
          </FieldLabel>

          <Controller
            name="mode"
            control={control}
            render={({ field }) => (
              <RadioGroup
                value={field.value}
                onValueChange={(v) => field.onChange(v)}
                className="flex"
              >
                {MODE_OPTIONS.map((op) => 
                <FieldLabel key={op.value}>
                  <Field orientation="horizontal">
                    <RadioGroupItem value={op.value} />
                    <FieldTitle className="text-xs"><op.icon className="w-3.5 h-3.5" />{op.label}</FieldTitle>
                  </Field>
                </FieldLabel>
                )}
              </RadioGroup>
            )}
          />

          {errors.mode && (
            <p className="text-xs text-red-500">{errors.mode.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            프롬프트 내용 <span className="text-red-500">*</span>
          </FieldLabel>
          <Textarea {...register("content")} placeholder="프롬프트 내용 입력" />
          {errors.content && (
            <p className="text-xs text-red-500">{errors.content.message}</p>
          )}
        </Field>
      </FieldGroup>
    </form>
  );
}
