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
import {
  Field,
  FieldGroup,
  FieldLabel,
  FieldTitle,
} from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { RadioGroup, RadioGroupItem } from "@/components/ui/radio-group";
import { Textarea } from "@/components/ui/textarea";
import type { Operation, Project } from "@/types";
import { SquareTerminal } from "lucide-react";
import { useState } from "react";

type CreatePromptDialogProps = {
  project: Project;
  open: boolean;
  onClose: () => void;
  onCreate: (version: string, operation: Operation, content: string) => void;
};

export function CreatePromptDialog({
  project,
  open,
  onClose,
  onCreate,
}: CreatePromptDialogProps) {
  const [version, setVersion] = useState<string>("");
  const [operation, setOperation] = useState<Operation>("prepare");
  const [content, setContent] = useState<string>("");

  const handleSubmit = () => {
    if (!version || !operation || !content) return;
    onCreate(version.trim(), operation, content.trim());
    setVersion("");
    setOperation("prepare");
    setContent("");
    onClose();
  };

  return (
    <Dialog open={open} onOpenChange={(v) => !v && onClose()}>
      <DialogContent className="sm:max-w-md gap-0 p-0 overflow-hidden rounded-2xl border-zinc-200">
        <DialogHeader className="px-6 pt-6 pb-4 border-b border-zinc-100">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-xl bg-violet-100 flex items-center justify-center">
              <SquareTerminal className="w-4 h-4 text-violet-600" />
            </div>
            <div>
              <DialogTitle className="text-sm font-semibold text-zinc-800">
                프롬프트 등록
              </DialogTitle>
              <DialogDescription className="text-xs">
                {project.name} · 새로운 버전의 프롬프트를 등록해주세요
              </DialogDescription>
            </div>
          </div>
        </DialogHeader>

        <FieldGroup className="px-6 py-5">
          <Field>
            <FieldLabel className="text-xs">
              프롬프트 버전 <span className="text-red-500">*</span>
            </FieldLabel>
            <Input
              onChange={(e) => setVersion(e.target.value)}
              name="version"
              value={version}
              placeholder="예) project-prepare-v1"
            />
          </Field>
          <Field>
            <FieldLabel className="text-xs">
              분석 작업
              <span className="text-red-500">*</span>
            </FieldLabel>
            <RadioGroup
              value={operation}
              onValueChange={(v: Operation) => setOperation(v)}
              className="flex"
            >
              <FieldLabel>
                <Field orientation="horizontal">
                  <RadioGroupItem value="prepare" />
                  <FieldTitle>전처리</FieldTitle>
                </Field>
              </FieldLabel>
              <FieldLabel>
                <Field orientation="horizontal">
                  <RadioGroupItem value="sentiment" />
                  <FieldTitle>감성 분석</FieldTitle>
                </Field>
              </FieldLabel>
            </RadioGroup>
          </Field>
          <Field>
            <FieldLabel className="ext-xs">
              프롬프트 내용
              <span className="text-red-500">*</span>
            </FieldLabel>
            <Textarea
              className="h-32"
              onChange={(e) => setContent(e.target.value)}
              name="content"
              value={content}
              placeholder="프롬프트 내용을 입력해주세요."
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
            disabled={!version || !operation || !content}
            onClick={handleSubmit}
          >
            등록
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
