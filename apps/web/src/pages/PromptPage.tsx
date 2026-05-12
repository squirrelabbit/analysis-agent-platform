import { useDatasetId } from "@/hooks/useDatasetId";
import type { Project } from "@/features/project/types/project";
import { useOutletContext } from "react-router-dom";
import { useDatasetDetail } from "@/features/dataset/hooks/useDataset";
import PromptHeader from "@/features/prompt/components/PromptHeader";
import { usePrompts } from "@/features/prompt/hooks/usePrompt";
import { MOCK_PROMPTS } from "@/mock/promptMockData";
import { useMemo, useState } from "react";
import {
  PromptDetailSkeleton,
  PromptListSkeleton,
} from "@/features/prompt/components/PromptSkeleton";
import { groupPrompts } from "@/features/prompt/utils/prompt";
import { PromptListPanel } from "@/features/prompt/components/PromptListPanel";
import { PromptDetailPanel } from "@/features/prompt/components/PromptDetailPanel";

export default function PromptPage() {
  const { project } = useOutletContext<{ project: Project }>();
  const { datasetId } = useDatasetId();
  const { data: dataset } = useDatasetDetail(project.id, datasetId);
  const { data: prompts = MOCK_PROMPTS, isLoading } = usePrompts();

  const groups = useMemo(() => groupPrompts(prompts), []);
  const [selectedKey, setSelectedKey] = useState<string | null>(null);

  const selectedGroup = groups.find((g) => g.groupKey === selectedKey) ?? null;

  const handleSaveNewVersion = (groupKey: string, content: string) => {
    // API 호출: POST /prompts  { ...currentPrompt, content }
    console.log("새 버전 저장:", groupKey, content);
  };

  if (!project || !dataset) return null;
  return (
    <div className="flex h-[calc(100vh-56px)]">
      <aside className="w-80 border-r bg-white flex flex-col">
        <PromptHeader project={project} dataset={dataset} />
        {isLoading ? (
          <PromptListSkeleton />
        ) : (
        <PromptListPanel
           groups={groups}
           selectedKey={selectedKey}
           onSelect={(g) => setSelectedKey(g.groupKey)}
         />
        )}
      </aside>
      <div className="flex-1 overflow-x-auto">
        {isLoading ? (
          <PromptDetailSkeleton />
        ) : (
          <PromptDetailPanel
           group={selectedGroup}
           onSaveNewVersion={handleSaveNewVersion}
         />
        )}
      </div>
    </div>
  );
}
