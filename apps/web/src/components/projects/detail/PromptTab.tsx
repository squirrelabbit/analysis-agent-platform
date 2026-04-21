import { usePrompt } from "@/hooks/usePrompt";
import type { Operation, Project } from "@/types";
import { PromptCard } from "../prompt/PromptCard";
import { EmptyForm } from "@/components/common/EmptyForm";
import { Plus, SquareTerminal } from "lucide-react";
import { useState } from "react";
import { CreatePromptDialog } from "../prompt/CreatePromptDialog";
import { Button } from "@/components/ui/button";

export default function PromptTab(props: Project) {
  const { prompts, addPrompt } = usePrompt(props.project_id);
  const [open, setOpen] = useState(false);

  const handleCreate = async (
    version: string,
    operation: Operation,
    content: string,
  ) => {
    await addPrompt({ version, operation, content });
  };

  return (
    <div>
      <div className="pb-2 flex justify-end">
        <Button size="sm" onClick={() => setOpen(true)} className="h-8 text-xs">
          <Plus className="w-3.5 h-3.5" />
          프롬프트 등록
        </Button>
      </div>
      {prompts.length === 0 ? (
        <EmptyForm
          title="등록된 프롬프트가 없습니다."
          description="프롬프트를 먼저 등록해주세요."
          icon={<SquareTerminal className="text-zinc-400" />}
        />
      ) : (
        <PromptCard {...prompts[0]} />
      )}

      <CreatePromptDialog
        project={props}
        open={open}
        onClose={() => setOpen(false)}
        onCreate={handleCreate}
      />
    </div>
  );
}
