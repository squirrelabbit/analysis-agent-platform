import { useState } from "react";
import { PromptReadView } from "./detail/PromptReadView";
import { PromptEditView } from "./detail/PromptEditView";
import { PromptDiffView } from "./detail/PromptDiffView";
import { PromptEmptyState } from "./PromptEmptyState";
import type { PromptGroup } from "../types/prompt";
import { PromptDetailHeader, type ViewMode } from "./detail/PromptDetailHeader";

interface Props {
  group: PromptGroup | null;
  onSaveNewVersion?: (groupKey: string, content: string) => void;
  onDelete?: (promptId: string) => void;
}

export function PromptDetailPanel({ group, onSaveNewVersion, onDelete }: Props) {
  const [activeVersion, setActiveVersion] = useState<string>(
    group?.latestVersion ?? ""
  );
  const [viewMode, setViewMode] = useState<ViewMode>("read");

  // group 바뀌면 최신 버전으로 리셋
  const effectiveVersion =
    group?.versions.find((v) => v.version === activeVersion)
      ? activeVersion
      : group?.latestVersion ?? "";

  const activePrompt = group?.versions.find(
    (v) => v.version === effectiveVersion
  );

  const previousPrompt = group && activePrompt
    ? group.versions[group.versions.indexOf(activePrompt) + 1] ?? null
    : null;

  if (!group || !activePrompt) {
    return (
      <div className="h-full flex-1 flex flex-col overflow-hidden">
        <PromptEmptyState />
      </div>
    );
  }

  const handleSave = (content: string) => {
    onSaveNewVersion?.(group.groupKey, content);
    setViewMode("read");
  };

  return (
    <div className="flex-1 flex flex-col overflow-hidden">
      <PromptDetailHeader
        group={group}
        activePrompt={activePrompt}
        viewMode={viewMode}
        onVersionChange={(v) => {
          setActiveVersion(v);
          setViewMode("read");
        }}
        onViewModeChange={setViewMode}
        onAddVersion={() => setViewMode("edit")}
        onDelete={() => onDelete?.(activePrompt.id)}
      />


      {viewMode === "read" && (
        <PromptReadView content={activePrompt.content} />
      )}

      {viewMode === "edit" && (
        <PromptEditView
          initialContent={activePrompt.content}
          onSave={handleSave}
          onCancel={() => setViewMode("read")}
        />
      )}

      {viewMode === "diff" && previousPrompt && (
        <PromptDiffView
          current={activePrompt}
          previous={previousPrompt}
        />
      )}

      {viewMode === "diff" && !previousPrompt && (
        <div className="flex-1 flex items-center justify-center
                        text-[12px] text-muted-foreground">
          비교할 이전 버전이 없습니다.
        </div>
      )}
    </div>
  );
}