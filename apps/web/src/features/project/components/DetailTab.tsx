import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import DatasetTab from "@/features/dataset/components/DatasetTab";

const TABS = [
  { key: "dataset", label: "데이터셋" },
  { key: "scenario", label: "시나리오" },
  { key: "prompt", label: "프롬프트" },
];

interface Props {
  tab: string;
  onChangeTab: (tab: string) => void;
  projectId: string;
}

export default function DetailTab({ tab, onChangeTab, projectId }: Props) {
  return (
    <Tabs value={tab} className="px-3">
      <TabsList variant={"line"} className="text-xs">
        {TABS.map(({ key, label }) => (
          <TabsTrigger
            key={key} 
            className="text-xs"
            value={key}
            onClick={() => onChangeTab(key)}
          >
            {label}
          </TabsTrigger>
        ))}
      </TabsList>
      <TabsContent value="dataset">
        <DatasetTab projectId={projectId} />
      </TabsContent>
    </Tabs>
  );
}
