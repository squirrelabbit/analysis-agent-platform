import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Plus, Search } from "lucide-react";
import { useState } from "react";
import { CreateProjectDialog } from "./CreateProjectDialog";

interface Props {
  search: string;
  onChangeSearch: (v: string) => void;
  filteredCount: number;
  totalCount: number;
  onCreate: (name: string, description: string) => void;
}

export default function ProjectHeader({
  search,
  onChangeSearch,
  filteredCount,
  totalCount,
  onCreate,
}: Props) {
  const [open, setOpen] = useState(false);

  return (
    <div>
      <div className="pb-3 flex justify-between items-center">
        <div>
          <h1 className="text-sm font-semibold text-zinc-800">전체 프로젝트</h1>
          <p className="text-[11px] text-zinc-400 mt-0.5">
            <span className="text-violet-500 font-medium">
              {filteredCount}개
            </span>
            {" / "}전체 {totalCount}개
          </p>
        </div>
        <Button onClick={() => setOpen(true)} className="text-xs">
          <Plus className="w-3.5 h-3.5" />
          프로젝트 등록
        </Button>
      </div>
      <div className="relative">
        <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-400" />
        <Input
          placeholder="Search..."
          value={search}
          onChange={(e) => onChangeSearch(e.target.value)}
          className="h-8 pl-8 text-xs rounded-lg border-zinc-200 bg-zinc-50 focus-visible:ring-violet-300"
        />
      </div>
      <CreateProjectDialog
        open={open}
        onClose={() => setOpen(false)}
        onCreate={onCreate}
      />
    </div>
  );
}
