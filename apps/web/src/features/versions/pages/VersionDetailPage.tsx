import { useVersion } from "../hooks/version.query";
import type { BuildJobType } from "@/shared/types/common";
import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import PipelineCard from "../components/PiplineCard";
import CleanTab from "../components/tabs/CleanTab";
import GenuinenessTab from "../components/tabs/GenuinenessTab";
import { ClauseTab } from "../components/tabs/ClauseTab";

export default function VersionDetailPage() {
  const { data: version } = useVersion();
  const [tab, setTab] = useState<BuildJobType>("clean");

  if (!version) return null;

  const builds = ["clean", "doc_genuineness", "clause_label"] as BuildJobType[];

  return (
    <div className="p-8">
      <div className="grid md:grid-cols-3 gap-4">
        {builds.map((build) => (
          <PipelineCard key={build} versionId={version.id} type={build} />
        ))}
      </div>
      <Tabs value={tab} onValueChange={(v) => setTab(v as BuildJobType)}>
        <TabsList className="mt-4">
          <TabsTrigger value="clean">데이터 정제</TabsTrigger>
          <TabsTrigger value="doc_genuineness">문서 진성 분석</TabsTrigger>
          <TabsTrigger value="clause_label">분류</TabsTrigger>
        </TabsList>
        <TabsContent value="clean">{version.clean && <CleanTab />}</TabsContent>
        <TabsContent value="doc_genuineness">
          {version.docGenuineness && <GenuinenessTab />}
        </TabsContent>
        <TabsContent value="clause_label">
          {version.clauseLabel && <ClauseTab />}
        </TabsContent>
      </Tabs>
    </div>
  );
}
