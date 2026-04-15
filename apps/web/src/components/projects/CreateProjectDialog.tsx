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
import { Field, FieldGroup, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { FolderOpen } from "lucide-react";
import { useState } from "react";

type CreateProjectDialogProps = {
  open: boolean;
  onClose: () => void;
  onCreate: (name: string, description: string) => void;
};

export function CreateProjectDialog({
  open,
  onClose,
  onCreate,
}: CreateProjectDialogProps) {
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");

  const handleSubmit = () => {
    if (!name || !description) return;
    onCreate(name.trim(), description.trim());
    setName("");
    setDescription("");
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="sm:max-w-sm gap-0 p-0 overflow-hidden rounded-2xl border-zinc-200">
        <DialogHeader className="px-6 pt-6 pb-4 border-b border-zinc-100">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-xl bg-violet-100 flex items-center justify-center">
              <FolderOpen className="w-4 h-4 text-violet-600" />
            </div>
            <div>
              <DialogTitle className="text-sm font-semibold text-zinc-800">
                프로젝트 등록
              </DialogTitle>
              <DialogDescription className="text-xs">
                신규 프로젝트를 등록하세요
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <FieldGroup className="px-6 py-5">
          <Field>
            <FieldLabel className="text-xs">
              프로젝트 이름 <span className="text-red-500">*</span>
            </FieldLabel>
            <Input
              onChange={(e) => setName(e.target.value)}
              name="name"
              placeholder="예) festival"
            />
          </Field>
          <Field>
            <FieldLabel className="text-xs">
              설명 <span className="text-red-500">*</span>
            </FieldLabel>
            <Input
              onChange={(e) => setDescription(e.target.value)}
              name="description"
              placeholder="프로젝트에 대한 간단한 설명"
            />
          </Field>
        </FieldGroup>

        <DialogFooter className="px-6 pb-6 gap-2 flex-row">
          <DialogClose asChild>
            <Button variant="outline" onClick={onClose}>
              취소
            </Button>
          </DialogClose>
          <Button
            disabled={!name || !description}
            onClick={handleSubmit}
          >
            등록
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
