import { useMemo, useState } from "react";
import type { Prompt } from "../types/prompt";
import { Braces, Search } from "lucide-react";
import { EmptyForm } from "@/components/common/EmptyForm";
import PromptItem from "./PromptItem";
import { Input } from "@/components/ui/input";

interface Props {
  prompts: Prompt[];
  selected?: string;
  onSelect: (id: string) => void;
}

export default function PromptList({ prompts, selected, onSelect }: Props) {
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    return prompts.filter((p) => p.title.trim().includes(search.trim()));
  }, [search]);

  if (prompts.length === 0) {
    return (
      <EmptyForm
        title="등록된 프롬프트가 없습니다"
        description="프롬프트를 등록해주세요"
        icon={<Braces className="text-zinc-400" />}
      />
    );
  }

  return (
    <>
      <div className="flex justify-between items-center py-3">
        <div className="relative w-100">
          <Search className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5 text-zinc-400" />
          <Input
            placeholder="Search..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            className="h-9 pl-8 text-xs rounded-md border-zinc-200 bg-white focus-visible:ring-indigo-300"
          />
        </div>
      </div>
      <div className="flex flex-col gap-2">
        {filtered.map((prompt) => (
          <PromptItem
            key={prompt.id}
            prompt={prompt}
            isSelected={selected === prompt.id}
            onSelect={() => onSelect(prompt.id)}
          />
        ))}
      </div>
    </>
  );
}
