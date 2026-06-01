import { AlertTriangle } from "lucide-react";
import type { TaxonomyStatus } from "../models";

const TAXONOMY_LABEL: Record<Exclude<TaxonomyStatus, "ok">, string> = {
  legacy_missing: "분류 체계 정보가 없는 과거 빌드입니다.",
  hash_mismatch: "분류 체계 해시가 일치하지 않습니다.",
  id_mismatch: "분류 체계 ID가 일치하지 않습니다.",
};

interface Props {
  warnings?: string[];
  taxonomyStatus?: TaxonomyStatus;
}

export default function MessageWarnings({ warnings, taxonomyStatus }: Props) {
  const items: string[] = [];
  if (warnings) items.push(...warnings);
  if (taxonomyStatus && taxonomyStatus !== "ok") {
    items.push(TAXONOMY_LABEL[taxonomyStatus]);
  }
  if (items.length === 0) return null;

  return (
    <ul className="mt-2 flex flex-col gap-1">
      {items.map((msg, idx) => (
        <li
          key={idx}
          className="flex items-start gap-1.5 rounded-md border border-amber-200 bg-amber-50 px-2.5 py-1.5 text-[11px] text-amber-700"
        >
          <AlertTriangle className="w-3 h-3 mt-0.5 shrink-0" />
          <span>{msg}</span>
        </li>
      ))}
    </ul>
  );
}
