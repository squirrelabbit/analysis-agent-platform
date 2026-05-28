import { Controller, useForm } from "react-hook-form";
import { datasetInfoSchema, type DatasetInfo } from "../../schemas/dataset";
import { zodResolver } from "@hookform/resolvers/zod";
import {
  Field,
  FieldDescription,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";

const TYPE_OPTIONS = [
  {
    value: "structured",
    label: "정형",
    desc: "CSV, 엑셀 등 테이블 구조 데이터",
  },
  {
    value: "unstructured",
    label: "비정형",
    desc: "문서, 리뷰, 텍스트 기반 데이터",
  },
];

export default function DatasetInfoForm({
  onSubmit,
  datasetInfo,
}: {
  onSubmit: (data: DatasetInfo) => void;
  datasetInfo: DatasetInfo | null;
}) {
  const {
    register,
    handleSubmit,
    control,
    formState: { errors },
  } = useForm<DatasetInfo>({
    resolver: zodResolver(datasetInfoSchema),
    defaultValues: datasetInfo ? datasetInfo : {
      dataType: "structured",
    },
  });

  return (
    <form id={`dataset-info-form`} onSubmit={handleSubmit(onSubmit)}>
      <FieldGroup>
        <Field>
          <FieldLabel className="text-xs">
            데이터셋 이름 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input {...register("name")} placeholder="예) sns 데이터" />
          {errors.name && (
            <p className="text-xs text-red-500">{errors.name.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            설명 <span className="text-red-500">*</span>
          </FieldLabel>
          <Input
            {...register("description")}
            placeholder="데이터셋에 대한 간단한 설명"
          />
          {errors.description && (
            <p className="text-xs text-red-500">{errors.description.message}</p>
          )}
        </Field>
        <Field>
          <FieldLabel className="text-xs">
            데이터 타입 <span className="text-red-500">*</span>
          </FieldLabel>
          <Controller
            control={control}
            name="dataType"
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
      </FieldGroup>
    </form>
  );
}
