import { useState } from "react";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { Pencil } from "lucide-react";
import CreateProjectForm from "../forms/CreateProjectForm";
import { useProjectRaw } from "../hooks/project.query";
import { useUpdateProjectMutation } from "../hooks/project.mutation";
import { projectToFormValues } from "../models/mapper";

// 프로젝트 수정 진입점(#31 후속). 이름/설명 + 축제 메타를 생성 폼(CreateProjectForm)을
// 재사용해 프리필·수정한다. 저장은 PATCH /projects/{id}(metadata는 백엔드에서 key 병합).
export default function EditProjectDialog({ projectId }: { projectId: string }) {
  const [open, setOpen] = useState(false);
  const formId = `project-edit-${projectId}`;

  // 열릴 때만 상세를 불러 축제 메타까지 프리필한다.
  const { data, isLoading } = useProjectRaw(projectId, open);
  const update = useUpdateProjectMutation();

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button
          variant="ghost"
          className="hover:bg-violet-50 hover:text-violet-600 text-zinc-400"
        >
          <Pencil />
        </Button>
      </DialogTrigger>

      <DialogContent className="sm:max-w-md flex flex-col max-h-[80vh]">
        <DialogHeader className="shrink-0">
          <DialogTitle>프로젝트 수정</DialogTitle>
        </DialogHeader>
        <div className="flex-1 overflow-y-auto">
          {isLoading || !data ? (
            <p className="px-6 py-8 text-center text-xs text-[#9399b0]">
              불러오는 중…
            </p>
          ) : (
            <CreateProjectForm
              formId={formId}
              defaultValues={projectToFormValues(data)}
              onSubmit={async (req) => {
                await update.mutateAsync({ id: projectId, req });
              }}
              onSuccess={() => setOpen(false)}
            />
          )}
        </div>
        <DialogFooter className="flex gap-2">
          <Button variant="outline" onClick={() => setOpen(false)}>
            취소
          </Button>
          <Button type="submit" form={formId} disabled={isLoading || !data}>
            저장
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
