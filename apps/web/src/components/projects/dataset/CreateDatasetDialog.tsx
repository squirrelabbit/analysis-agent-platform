import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Field, FieldGroup, FieldLabel, FieldTitle } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Database } from "lucide-react";
import { useState } from "react";

type CreateDatasetDialogProps = {
  open: boolean;
  onClose: () => void;
  onCreate: (name: string, description: string, data_type: string) => void;
};

export function CreateDatasetDialog({
  open,
  onClose,
  onCreate,
}: CreateDatasetDialogProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [type, setType] = useState("");

  const handleSubmit = () => {
    if (!name || !description || !type) return;
    onCreate(name.trim(), description.trim(), type.trim());
    setName("");
    setDescription("");
    setType("")
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="sm:max-w-sm gap-0 p-0 overflow-hidden rounded-2xl border-zinc-200">
        <DialogHeader className="px-6 pt-6 pb-4 border-b border-zinc-100">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-xl bg-violet-100 flex items-center justify-center">
              <Database className="w-4 h-4 text-violet-600" />
            </div>
            <div>
              <DialogTitle className="text-sm font-semibold text-zinc-800">
                데이터셋 등록
              </DialogTitle>
              <DialogDescription className="text-xs">
                이름을 먼저 등록한 뒤 파일을 업로드하세요
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <FieldGroup className="px-6 py-5">
          <Field>
            <FieldLabel className="text-xs">
              데이터셋 이름 <span className="text-red-500">*</span>
            </FieldLabel>
            <Input
              onChange={(e) => setName(e.target.value)}
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
              onChange={(e) => setDescription(e.target.value)}
              name="description"
              placeholder="데이터셋에 대한 간단한 설명"
            />
          </Field>
          <Field>
            <FieldLabel className="text-xs">
              타입
              <span className="text-red-500">*</span>
            </FieldLabel>
            <RadioGroup defaultValue="unstructured" orientation="vertical">
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

        <DialogFooter className="px-6 pb-6 gap-2 flex-row">
          <DialogClose asChild>
            <Button variant="outline" onClick={onClose}>
              취소
            </Button>
          </DialogClose>
          <Button
            disabled={!name || !description || !type}
            onClick={handleSubmit}
          >
            등록
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
