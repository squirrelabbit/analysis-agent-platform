import { useState } from "react";
import { DataTable, type Column } from "./DataTable";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import ChatToast from "@/features/chats/components/ChatToast";
import {
  useKeywordDictionaryRules,
  useToggleKeywordDictionaryRule,
} from "../hooks/build.query";
import type { KeywordDictionaryRule } from "../models/build";

const fmtDate = (iso: string) => (iso ? iso.slice(0, 10) : "-");

// 정제 규칙 탭 (silverone 2026-06-25) — dataset 단위 block/synonym 규칙 목록.
// 비활성 포함 조회(include_inactive)해 해제된 규칙도 보이고 재활성 가능.
export default function KeywordRulesTab() {
  const { data: rules = [], isLoading } = useKeywordDictionaryRules(true);
  const { mutateAsync, isPending } = useToggleKeywordDictionaryRule();
  const [toast, setToast] = useState("");

  const toggle = async (r: KeywordDictionaryRule) => {
    await mutateAsync({ ruleId: r.id, active: !r.active });
    setToast(
      r.active
        ? `"${r.sourceTerm}" 규칙을 해제했습니다.`
        : `"${r.sourceTerm}" 규칙을 재활성했습니다.`,
    );
    setTimeout(() => setToast(""), 2000);
  };

  const columns: Column<KeywordDictionaryRule>[] = [
    {
      header: "유형",
      cell: (r) => (
        <td className="px-3 py-2">
          <Badge variant="outline">
            {r.ruleType === "block" ? "제외" : "병합"}
          </Badge>
        </td>
      ),
    },
    {
      header: "원 키워드",
      cell: (r) => <td className="px-3 py-2 font-medium">{r.sourceTerm}</td>,
    },
    {
      header: "대표어",
      cell: (r) => (
        <td className="px-3 py-2 text-slate-600">{r.targetTerm || "-"}</td>
      ),
    },
    {
      header: "상태",
      cell: (r) => (
        <td className="px-3 py-2">
          <Badge variant={r.active ? "default" : "outline"}>
            {r.active ? "활성" : "해제됨"}
          </Badge>
        </td>
      ),
    },
    {
      header: "생성일",
      cell: (r) => (
        <td className="px-3 py-2 text-slate-500">{fmtDate(r.createdAt)}</td>
      ),
    },
    {
      header: "액션",
      cell: (r) => (
        <td className="px-3 py-2">
          <Button
            size="xs"
            variant="ghost"
            disabled={isPending}
            onClick={() => toggle(r)}
          >
            {r.active ? "해제" : "재활성"}
          </Button>
        </td>
      ),
    },
  ];

  return (
    <>
      <DataTable
        columns={columns}
        items={rules}
        rowKey={(r) => r.id}
        title={
          <span className="text-sm font-semibold">
            정제 규칙 <span className="text-slate-400">{rules.length}</span>
          </span>
        }
        emptyText="아직 정제 규칙이 없습니다. 키워드 결과에서 [제외]·[대표어 지정]으로 추가하세요."
        page={1}
        totalPages={1}
        totalCount={rules.length}
        onPageChange={() => {}}
        loading={isLoading}
      />
      <ChatToast message={toast} visible={!!toast} />
    </>
  );
}
