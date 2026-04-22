import type { Project } from "../types/project";
import { useSearchParams } from "react-router-dom";
import {
  Item,
  ItemContent,
  ItemDescription,
  ItemTitle,
} from "@/components/ui/item";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import DatasetTab from "@/features/dataset/components/DatasetTab";
import { Badge } from "@/components/ui/badge";

export default function ProjectDetailPanel(props: Project) {
  const [searchParams, setSearchParams] = useSearchParams();
  const tab = searchParams.get("tab") ?? "dataset";

  const { id, name, description, datasetCount, scenarioCount, promptCount } =
    props;

  const TABS = [
    { key: "dataset", label: "데이터셋", count: datasetCount },
    { key: "scenario", label: "시나리오", count: scenarioCount },
    { key: "prompt", label: "프롬프트", count: promptCount },
  ];

  const changeTab = (tab: string) => {
    setSearchParams({ tab });
  };

  return (
    <div className="flex flex-col h-full">
      <Item>
        <ItemContent>
          <ItemTitle className="font-bold text-lg">{name}</ItemTitle>
          <ItemDescription className="text-xs">{description}</ItemDescription>
        </ItemContent>
      </Item>
      <Tabs value={tab} className="px-3">
        <TabsList variant={"line"} className="text-xs">
          {TABS.map(({ key, label, count }) => (
            <TabsTrigger
              key={key} // text-violet-500 active:text-violet-500
              className="text-xs"
              value={key}
              onClick={() => changeTab(key)}
            >
              {label}{" "}
              <Badge variant={count == 0 ? "destructive" : "default"}>
                {count}
              </Badge>
            </TabsTrigger>
          ))}
        </TabsList>
        <TabsContent value="dataset">
          <DatasetTab projectId={id} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
