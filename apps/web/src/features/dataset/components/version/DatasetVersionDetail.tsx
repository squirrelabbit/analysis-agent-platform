import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { DatasetVersion } from "../../types/datasetVersion";
import DataInfoTab from "./tabs/DataInfoTab";
import PiplineTab from "./tabs/PiplineTab";
import { AnalysisResultTab } from "./tabs/AnalysisResultTab";
import { Item, ItemContent, ItemHeader, ItemTitle } from "@/components/ui/item";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { CheckCircle2 } from "lucide-react";

export function DatasetVersionDetail({ version }: { version: DatasetVersion }) {
  return (
    <Item>
      <ItemHeader>
        <ItemTitle>
          <div className="flex-1 flex items-center gap-4 py-3 flex-wrap">
            <div>
              <p className="text-[12px] font-mono font-medium text-foreground">
                {version.metadata.upload.original_filename}
              </p>
              <p className="text-[11px] text-muted-foreground mt-0.5">
                업로드 {version.metadata.upload.uploaded_at.slice(0, 10)}
              </p>
            </div>

            <div className="flex gap-2 items-center flex-wrap">
              <Badge
                variant="outline"
                className={cn(
                  "h-5 px-2 text-[10px] font-semibold",
                  version.isActive
                    ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                    : "bg-muted/50 text-muted-foreground",
                )}
              >
                {" "}
                {version.isActive && (
                  <CheckCircle2 className="w-2.5 h-2.5 mr-1" />
                )}
                {version.isActive ? "활성" : "비활성"}
              </Badge>
              <Badge
                variant="outline"
                className="border-blue-200 bg-blue-50 text-blue-600 text-[10px] font-semibold"
              >
                {version.dataType === "structured" ? '정형' : '비정형'}
              </Badge>
            </div>
          </div>
        </ItemTitle>
      </ItemHeader>
      <ItemContent>
        <Tabs defaultValue="info" >
          <TabsList variant="line">
            <TabsTrigger value="info">데이터 정보</TabsTrigger>
            <TabsTrigger value="pipeline">파이프라인</TabsTrigger>
            <TabsTrigger value="result">분석 결과</TabsTrigger>
          </TabsList>
          <TabsContent value="info">
            <DataInfoTab {...version} />
          </TabsContent>
          <TabsContent value="pipeline">
            <PiplineTab version={version} />
          </TabsContent>
          <TabsContent value="result">
            <AnalysisResultTab version={version} />
          </TabsContent>
        </Tabs>
      </ItemContent>
    </Item>
  );
}
