import {
  InputGroup,
  InputGroupAddon,
  InputGroupInput,
} from "../ui/input-group";
import { Plus, Search } from "lucide-react";
import { useState } from "react";
import { CreateProjectDialog } from "./CreateProjectDialog";
import { Button } from "../ui/button";
import type { CreateProjectPayload } from "@/types/dto/project.dto";

interface ProjectHeadProps {
  total: number;
  filteredCount: number;
  searchQuery: string;
  onSearchChange: (query: string) => void;
  onAddProject: (payload: CreateProjectPayload) => void
}

export default function ProjectHead({
  total,
  filteredCount,
  searchQuery,
  onSearchChange,
  onAddProject
}: ProjectHeadProps) {
  const [open, setOpen] = useState(false);

  const handleCreate = (name: string, description: string) => {
    onAddProject({ name, description });
  };

  return (
    <div className="flex flex-col gap-2 pb-2">
      <div className="flex justify-between items-center">
        <div>
          <div>전체 프로젝트</div>
          <p className="text-[11px] text-zinc-400 mt-0.5">
            <span className="text-violet-500 font-medium">
              {filteredCount}개
            </span>
            {" / "}전체 {total}개
          </p>
        </div>
        <Button size="sm" onClick={() => setOpen(true)} className="h-8 text-xs">
          <Plus className="w-3.5 h-3.5" />
          프로젝트 등록
        </Button>
      </div>
      <InputGroup>
        <InputGroupInput
          value={searchQuery}
          onChange={(e) => onSearchChange(e.target.value)}
          placeholder="Search..."
        />
        <InputGroupAddon>
          <Search />
        </InputGroupAddon>
      </InputGroup>

      <CreateProjectDialog
        open={open}
        onClose={() => setOpen(false)}
        onCreate={handleCreate}
      />
    </div>
  );
}
