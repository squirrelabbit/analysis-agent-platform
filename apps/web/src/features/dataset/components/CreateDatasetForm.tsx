import {
  Field,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import type { DatasetForm } from "../types/dataset.form";

export default function CreateDatasetForm({ onChange }: {onChange:(key: keyof DatasetForm, value: string) => void}) {
  return (
    <FieldGroup className="px-6 py-5">
      <Field>
        <FieldLabel className="text-xs">
          데이터셋 이름 <span className="text-red-500">*</span>
        </FieldLabel>
        <Input
          onChange={(e) => onChange('name', e.target.value)}
          name="name"
          placeholder="예) sns 데이터"
        />
      </Field>
      <Field>
        <FieldLabel className="text-xs">
          설명 <span className="text-red-500">*</span>
          {/* <span className="text-zinc-400 font-normal">(선택)</span> */}
        </FieldLabel>
        <Input
          onChange={(e) => onChange('description', e.target.value)}
          name="description"
          placeholder="데이터셋에 대한 간단한 설명"
        />
      </Field>
      <Field>
        <FieldLabel className="text-xs">
          타입
          <span className="text-red-500">*</span>
        </FieldLabel>
        <RadioGroup
          onValueChange={(v) => onChange('dataType',v)}
          defaultValue="unstructured"
        >
          <FieldLabel>
            <Field orientation="horizontal">
              <RadioGroupItem value="structured" />
              <FieldTitle>정형</FieldTitle>
            </Field>
          </FieldLabel>
          <FieldLabel>
            <Field orientation="horizontal">
              <RadioGroupItem value="unstructured" />
              <FieldTitle>비정형</FieldTitle>
            </Field>
          </FieldLabel>
        </RadioGroup>
      </Field>
    </FieldGroup>
  );
}
