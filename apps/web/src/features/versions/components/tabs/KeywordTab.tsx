import { useState } from "react";
import { Hash, Layers, Percent, Star } from "lucide-react";
import { StatCard } from "@/components/common/cards/StatCard";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import {
  BuildMetaBar,
  BuildRunningBanner,
  BuildTabEmpty,
  BuildTabLoading,
  isBuildRunning,
} from "../BuildStatusMeta";
import type { KeywordBuild } from "../../models/build";
import { useBuildVersion } from "../../hooks/build.query";
import KeywordClauseTable from "../KeywordClauseTable";
import KeywordSentimentRankTable from "../KeywordSentimentRankTable";
import KeywordRefineDialog, { type RefineMode } from "../KeywordRefineDialog";
import KeywordRulesTab from "../KeywordRulesTab";
import KeywordHistoryTab from "../KeywordHistoryTab";
import ChatToast from "@/features/chats/components/ChatToast";

export function KeywordTab() {
  const { data, isLoading } = useBuildVersion("clause_keywords") as {
    data: KeywordBuild | undefined;
    isLoading: boolean;
  };
  const {
    summary,
    items = [],
    applied,
    status,
    progress,
    durationSeconds,
  } = data || {};

  // 키워드 정제 모달 상태 + 저장 피드백 토스트.
  const [refine, setRefine] = useState<{
    open: boolean;
    mode: RefineMode;
    keyword: string;
  }>({ open: false, mode: "exclude", keyword: "" });
  const [toast, setToast] = useState("");
  const openRefine = (keyword: string, mode: RefineMode) =>
    setRefine({ open: true, mode, keyword });
  const showToast = (msg: string) => {
    setToast(msg);
    setTimeout(() => setToast(""), 2200);
  };

  if (isLoading) return <BuildTabLoading />;

  if (!summary && !isBuildRunning(status)) {
    return (
      <BuildTabEmpty type="clause_keywords" status={status ?? "not_requested"} />
    );
  }

  const topTerms = items.slice(0, 2).map((it) => it.keyword);

  return (
    <Tabs defaultValue="result" className="space-y-4">
      <TabsList variant="line">
        <TabsTrigger value="result">키워드 결과</TabsTrigger>
        <TabsTrigger value="rules">정제 규칙</TabsTrigger>
        <TabsTrigger value="history">변경 이력</TabsTrigger>
      </TabsList>

      <TabsContent value="result" className="space-y-5">
        {summary && (
          <>
            <BuildMetaBar
              status={status}
              durationSeconds={durationSeconds}
              applied={applied}
            />
            <div className="grid grid-cols-2 gap-3.5 sm:grid-cols-4">
              <StatCard
                value={summary.totalKeywordCount.toLocaleString()}
                label="총 키워드 추출"
                icon={Hash}
                tone="neutral"
              />
              <StatCard
                value={summary.uniqueKeywordCount.toLocaleString()}
                label="고유 키워드 수"
                icon={Layers}
                tone="blue"
              />
              <StatCard
                value={topTerms.join(" · ") || "-"}
                label="최다 출현 키워드"
                icon={Star}
                tone="muted"
              />
              <StatCard
                value={summary.clauseCount.toLocaleString()}
                label="분석 절 수"
                icon={Percent}
                tone="ok"
                valueColor="text-emerald-600"
              />
            </div>
          </>
        )}

        {!summary && isBuildRunning(status) && (
          <BuildRunningBanner
            status={status}
            progress={progress}
            hasPrevious={false}
          />
        )}

        {/* 키워드별 긍정/부정 순위 — 각 행에서 바로 [병합]/[제외] 정제 */}
        <KeywordSentimentRankTable onRefine={openRefine} />

        {/* 절에서 추출된 키워드 */}
        <KeywordClauseTable />
      </TabsContent>

      <TabsContent value="rules">
        <KeywordRulesTab />
      </TabsContent>

      <TabsContent value="history">
        <KeywordHistoryTab />
      </TabsContent>

      <KeywordRefineDialog
        open={refine.open}
        onOpenChange={(open) => setRefine((s) => ({ ...s, open }))}
        mode={refine.mode}
        sourceTerm={refine.keyword}
        onSaved={showToast}
      />
      <ChatToast message={toast} visible={!!toast} />
    </Tabs>
  );
}
