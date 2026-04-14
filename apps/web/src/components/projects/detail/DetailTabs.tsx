import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import DatasetTab from "./DatasetTab";
import ScenarioTab from "./ScenarioTab";
import PromptTab from "./PromptTab";

export default function DetailTabs() {
  return (
    <Tabs defaultValue="dataset">
        <TabsList variant="line" >
          <TabsTrigger value="dataset" 
          // className=" data-[state=active]:text-[#574ae4]"
          >데이터셋</TabsTrigger>
          <TabsTrigger value="scenario">시나리오</TabsTrigger>
          <TabsTrigger value="prompt">프롬프트</TabsTrigger>
        </TabsList>
        <TabsContent value="dataset">
          <DatasetTab />
        </TabsContent>
        <TabsContent value="scenario">
          <ScenarioTab />
        </TabsContent>
        <TabsContent value="prompt">
          <PromptTab />
        </TabsContent>
      </Tabs>
  )
}