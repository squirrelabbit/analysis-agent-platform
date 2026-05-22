import {
  Field,
  FieldContent,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { zodResolver } from "@hookform/resolvers/zod";
import { Controller, useFieldArray, useForm } from "react-hook-form";
import {
  versionSchema,
  type UploadVersionFormValues,
} from "../../../schcema/dataset.schcema";
import { Plus, X } from "lucide-react";
import { cn } from "@/lib/utils";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Input } from "@/components/ui/input";
import FileUploader from "@/components/common/files/FileUploader";
import { CleanOptionsAccordion } from "./CleanOptionsAccordion";
import { Button } from "@/components/ui/button";
import { Switch } from "@/components/ui/switch";

interface UploadVersionFormProps {
  formId: string;
  type: "structured" | "unstructured";
  onSubmit: (data: UploadVersionFormValues) => Promise<void>;
  onSuccess: () => void;
}

// const ANALYSIS_OPTIONS: { value: any; label: string; desc: string }[] = [
//   { value: "sentiment", label: "감성 분석", desc: "텍스트 긍/부정 분류" },
//   { value: "prepare", label: "전처리", desc: "정제 및 정규화" },
//   { value: "embedding", label: "임베딩", desc: "벡터 변환" },
// ];

const TYPE_OPTIONS = [
  { value: "structured", label: "정형", desc: "CSV, 테이블 형식" },
  { value: "unstructured", label: "비정형", desc: "텍스트, JSON 자유형" },
];

export default function UploadVersionForm({
  formId,
  type,
  onSubmit,
  onSuccess,
}: UploadVersionFormProps) {
  const {
    register,
    handleSubmit,
    control,
    // watch,
    formState: { errors },
  } = useForm<UploadVersionFormValues>({
    resolver: zodResolver(versionSchema),
    defaultValues: {
      dataType: type,
      activateOnCreate: true
    },
  });

  async function handleFormSubmit(data: UploadVersionFormValues) {
    await onSubmit(data);
    onSuccess();
  }

  return (
    <form id={formId} onSubmit={handleSubmit(handleFormSubmit)}>
      <FieldGroup className="px-3">
        <Field>
          <FieldLabel className="text-xs">
            파일 <span className="text-red-500">*</span>
          </FieldLabel>
          <Controller
            name="file"
            control={control}
            render={({ field }) => (
              <FileUploader value={field.value} onChange={field.onChange} />
            )}
          />
          {errors.file && (
            <p className="text-xs text-red-500">{errors.file.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            데이터 타입 <span className="text-red-500">*</span>
          </FieldLabel>
          <Controller
            control={control}
            name="dataType"
            defaultValue={type}
            render={({ field }) => (
              <RadioGroup
                value={field.value}
                onValueChange={(v) =>
                  field.onChange(v as "structured" | "unstructured")
                }
                className="flex"
              >
                {TYPE_OPTIONS.map((opt) => (
                  <FieldLabel key={opt.value}>
                    <Field orientation="horizontal">
                      <RadioGroupItem value={opt.value} />
                      <div>
                        <FieldTitle className="text-xs">{opt.label}</FieldTitle>
                        <FieldDescription className="text-[10px] text-zinc-400">
                          {opt.desc}
                        </FieldDescription>
                      </div>
                    </Field>
                  </FieldLabel>
                ))}
              </RadioGroup>
            )}
          />
          {errors.dataType && (
            <p className="text-xs text-red-500">{errors.dataType.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">자동 클렌징 실행<p className="text-xs text-zinc-300">(선택)</p></FieldLabel>
          <Controller
            control={control}
            name="activateOnCreate"
            render={({ field }) => (
              <Switch id="switch-notifications" defaultChecked />
      //         <FieldLabel htmlFor="switch-notifications">
      //   <Field orientation="horizontal">
      //     <FieldContent>
      //       <FieldTitle>Enable notifications</FieldTitle>
      //       <FieldDescription>
      //         Receive notifications when focus mode is enabled or disabled.
      //       </FieldDescription>
      //     </FieldContent>
      //     <Switch id="switch-notifications" defaultChecked />
      //   </Field>
      // </FieldLabel>
            )}
          />
        </Field>
       
      </FieldGroup>
    </form>
  );
}
