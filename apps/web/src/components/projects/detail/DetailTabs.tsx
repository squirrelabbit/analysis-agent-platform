import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import DatasetTab from "./DatasetTab";
import ScenarioTab from "./ScenarioTab";
import PromptTab from "./PromptTab";
import type { Project } from "@/types";

export default function DetailTabs(props: Project) {
  return (
    <Tabs defaultValue="dataset">
      <TabsList variant="line">
        <TabsTrigger value="dataset">데이터셋</TabsTrigger>
        <TabsTrigger value="scenario">시나리오</TabsTrigger>
        <TabsTrigger value="prompt">프롬프트</TabsTrigger>
      </TabsList>
      <TabsContent value="dataset">
        <DatasetTab {...props} />
      </TabsContent>
      <TabsContent value="scenario">
        <ScenarioTab {...props} />
      </TabsContent>
      <TabsContent value="prompt">
        <PromptTab {...props} />
      </TabsContent>
    </Tabs>
  );
}
