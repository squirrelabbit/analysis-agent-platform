import { OPERATION_GROUP_ORDER } from "../config/prompt";
import type { Prompt, PromptGroup } from "../types/prompt";

/** API 응답 배열 → 그룹핑 */
export function groupPrompts(prompts: Prompt[]): PromptGroup[] {
  const map = new Map<string, Prompt[]>();

  prompts.forEach((p) => {
    const key = `${p.operation}::${p.title}`;
    if (!map.has(key)) map.set(key, []);
    map.get(key)!.push(p);
  });

  const groups: PromptGroup[] = [];

  map.forEach((versions, key) => {
    // 최신순 정렬 (version string: v0.1, v0.4 등)
    const sorted = [...versions].sort((a, b) =>
      parseFloat(b.version.replace("v", "")) -
      parseFloat(a.version.replace("v", ""))
    );
    const latest = sorted[0];
    groups.push({
      groupKey: key,
      title: latest.title,
      operation: latest.operation,
      latestVersion: latest.version,
      summary: latest.summary,
      updatedAt: latest.updatedAt,
      versions: sorted,
    });
  });

  // operation 순서 기준 정렬
  return groups.sort(
    (a, b) =>
      OPERATION_GROUP_ORDER.indexOf(a.operation) -
      OPERATION_GROUP_ORDER.indexOf(b.operation)
  );
}

export function fmtDate(iso: string): string {
  if (!iso) return "—";
  return iso.slice(0, 10);
}