import { useMemo, useState } from "react";
import { PromptFilterBar } from "./PromptFilterBar";
import { PromptGroupHeader } from "./PromptGroupHeader";
import { PromptListItem } from "./PromptListItem";
import type { PromptGroup, PromptOperation } from "../types/prompt";
import { OPERATION_GROUP_ORDER } from "../config/prompt";

interface Props {
  groups: PromptGroup[];
  selectedKey: string | null;
  onSelect: (group: PromptGroup) => void;
}

export function PromptListPanel({ groups, selectedKey, onSelect }: Props) {
  const [search, setSearch] = useState("");
  const [activeOp, setActiveOp] = useState<PromptOperation | "all">("all");

  const filtered = useMemo(() => {
    return groups.filter((g) => {
      const opMatch = activeOp === "all" || g.operation === activeOp;
      const searchMatch =
        !search ||
        g.title.toLowerCase().includes(search.toLowerCase()) ||
        g.operation.includes(search.toLowerCase());
      return opMatch && searchMatch;
    });
  }, [groups, search, activeOp]);

  // operation 순서 기준 그룹핑
  const byOp = useMemo(() => {
    const map = new Map<PromptOperation, PromptGroup[]>();
    OPERATION_GROUP_ORDER.forEach((op) => map.set(op, []));
    filtered.forEach((g) => map.get(g.operation)?.push(g));
    return map;
  }, [filtered]);

  return (
    <div className="">
      <PromptFilterBar
        search={search}
        activeOp={activeOp}
        onSearchChange={setSearch}
        onOpChange={setActiveOp}
      />

      <div className="flex-1 overflow-y-auto">
        {OPERATION_GROUP_ORDER.map((op) => {
          const items = byOp.get(op) ?? [];
          if (!items.length) return null;
          return (
            <div key={op}>
              <PromptGroupHeader operation={op} count={items.length} />
              {items.map((g) => (
                <PromptListItem
                  key={g.groupKey}
                  group={g}
                  selected={selectedKey === g.groupKey}
                  onSelect={() => onSelect(g)}
                />
              ))}
            </div>
          );
        })}

        {!filtered.length && (
          <div className="flex items-center justify-center py-16 text-[12px] text-muted-foreground">
            결과 없음
          </div>
        )}
      </div>
    </div>
  );
}
