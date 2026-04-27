import { Item, ItemContent, ItemHeader, ItemTitle } from "@/components/ui/item";
import type { DatasetVersion } from "../../types/datasetVersion";
import { Badge } from "@/components/ui/badge";
import { Activity, CheckCircle2, FileText, RotateCcw } from "lucide-react";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { cn } from "@/lib/utils";
import FileInfoTab from "./FileInfoTab";
import AnalysisSummaryTab from "./AnalysisSummaryTab";
import { Button } from "@/components/ui/button";

export default function DatasetVersionDetail({
  version,
}: {
  version: DatasetVersion;
}) {
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
                정형
              </Badge>
            </div>
          </div>
        </ItemTitle>
        <div className="ml-auto shrink-0">
          <Button
            variant="outline"
            size="sm"
            className="h-7 text-[11px] gap-1.5"
          >
            <RotateCcw className="w-3 h-3" />
            재실행
          </Button>
        </div>
      </ItemHeader>
      <ItemContent>
        <Tabs defaultValue="data">
          <TabsList>
            <TabsTrigger value="data">
              {" "}
              <FileText className="w-3 h-3" />
              데이터 정보
            </TabsTrigger>
            <TabsTrigger value="summary">
              {" "}
              <Activity className="w-3 h-3" />
              분석 요약
            </TabsTrigger>
          </TabsList>
          <TabsContent value="data">
            <FileInfoTab {...version} />
          </TabsContent>
          <TabsContent value="summary">
            <AnalysisSummaryTab {...version} />
            <div className="mt-4 pt-4 border-t border-border">
              <p className="text-[10px] font-mono text-muted-foreground uppercase tracking-wider mb-1.5">
                적용 프롬프트
              </p>
              <Badge
                variant="outline"
                className="text-[11px] border-purple-200 bg-purple-50 text-purple-700"
              >
                {version.preparePromptVersion}
              </Badge>
            </div>
          </TabsContent>
        </Tabs>
      </ItemContent>
    </Item>
  );
}
