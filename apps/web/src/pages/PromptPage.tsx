import { useDatasetId } from "@/hooks/useDatasetId";
import type { Project } from "@/features/project/types/project";
import { useOutletContext } from "react-router-dom";
import { useDatasetDetail } from "@/features/dataset/hooks/useDataset";
import PromptHeader from "@/features/prompt/components/PromptHeader";
import PromptList from "@/features/prompt/components/PromptList";
import { usePrompts } from "@/features/prompt/hooks/usePrompt";
import { MOCK_PROMPTS } from "@/mock/promptMockData";
import { useMemo, useState } from "react";
import PromptDetail from "@/features/prompt/components/PromptDetail";
import {
  PromptDetailSkeleton,
  PromptListSkeleton,
} from "@/features/prompt/components/PromptSkeleton";

export default function PromptPage() {
  const { project } = useOutletContext<{ project: Project }>();
  const { datasetId } = useDatasetId();
  const { data: dataset } = useDatasetDetail(project.id, datasetId);
  const { data: prompts = MOCK_PROMPTS, isLoading } = usePrompts();

  const [selected, setSelected] = useState<string>(prompts[0]?.id);
  const prompt = useMemo(
    () => prompts.find((p) => p.id === selected),
    [selected],
  );

  if (!project || !dataset) return null;
  return (
    <div className="flex h-[calc(100vh-56px)]">
      <aside className="w-80 border-r bg-white flex flex-col p-4">
        <PromptHeader project={project} dataset={dataset} />
        {isLoading ? (
          <PromptListSkeleton />
        ) : (
          <PromptList
            prompts={prompts}
            selected={selected}
            onSelect={(id) => setSelected(id)}
          />
        )}
      </aside>
      <div className="flex-1 overflow-x-auto">
        {isLoading ? (
          <PromptDetailSkeleton />
        ) : (
          prompt && <PromptDetail prompt={prompt} />
        )}
      </div>
    </div>
  );
}
