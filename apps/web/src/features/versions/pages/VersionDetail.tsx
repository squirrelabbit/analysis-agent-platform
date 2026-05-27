import { useVersion } from "../hooks/version.query";
import type { BuildJobType } from "@/shared/types/common";
import { useState } from "react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import PipelineCard from "../components/PiplineCard";
import { useBuildVersion } from "../hooks/build.query";
import CleanTab from "../components/tabs/CleanTab";
import type { VersionDetail } from "../models/version";
import GenuinenessTab from "../components/tabs/GenuinenessTab";
import { ClauseTab } from "../components/tabs/ClauseTab";
import type { ClauseBuild, CleanBuild, GenuinenessBuild } from "../models/build";

export default function VersionDetail() {
  const { data: version } = useVersion();
  const [tab, setTab] = useState<BuildJobType>("clause_label");

  const { data: buildResult, isLoading } = useBuildVersion(tab);

  if (!version) return null;

  const builds = ["clean", "doc_genuineness", "clause_label"] as BuildJobType[];

  if (!buildResult || isLoading) return null;
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
        <div className="mt-3 rounded-xl border border-zinc-100 bg-white p-5">
          <TabsContent value="clean">
            {version.clean && <CleanTab clean={buildResult as CleanBuild} />}
          </TabsContent>
          <TabsContent value="doc_genuineness">
            {tab === 'doc_genuineness' &&(
              <GenuinenessTab docGenuineness={buildResult as GenuinenessBuild} />
            )}
          </TabsContent>
          <TabsContent value="clause_label">
            <ClauseTab clauseLabel={buildResult as ClauseBuild} />
          </TabsContent>
        </div>
      </Tabs>
    </div>
  );
}
