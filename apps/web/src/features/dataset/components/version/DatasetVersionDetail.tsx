import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import type { DatasetVersion, DatasetVersionDetail } from "../../types/datasetVersion";
import DataInfoTab from "./tabs/DataInfoTab";
import PiplineTab from "./tabs/PiplineTab";
import { AnalysisResultTab } from "./tabs/AnalysisResultTab";
import { Item, ItemContent, ItemHeader, ItemTitle } from "@/components/ui/item";
import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import { CheckCircle2 } from "lucide-react";
import { fmtDate } from "@/utils/format";

export function DatasetVersionDetail({ version, detail }: { version: DatasetVersion, detail: DatasetVersionDetail }) {
  const { createdAt, isActive } = detail
  const { originalFilename } = version
  
  return (
    <Item>
      <ItemHeader>
        <ItemTitle>
          <div className="flex-1 flex items-center gap-4 py-3 flex-wrap">
            <div>
              <p className="text-[12px] font-mono font-medium text-foreground">
                {originalFilename}
              </p>
              <p className="text-[11px] text-muted-foreground mt-0.5">
                업로드 {fmtDate(createdAt)}
              </p>
            </div>

            <div className="flex gap-2 items-center flex-wrap">
              <Badge
                variant="outline"
                className={cn(
                  "h-5 px-2 text-[10px] font-semibold",
                  isActive
                    ? "border-emerald-200 bg-emerald-50 text-emerald-700"
                    : "bg-muted/50 text-muted-foreground",
                )}
              >
                {" "}
                {isActive && (
                  <CheckCircle2 className="w-2.5 h-2.5 mr-1" />
                )}
                {isActive ? "활성" : "비활성"}
              </Badge>
            </div>
          </div>
        </ItemTitle>
      </ItemHeader>
          {/* clean            → 데이터 정제
doc_genuineness  → 문서 품질 검증
clause_label     → 문장 분류 */}
      <ItemContent>
        <Tabs defaultValue="info" >
          <TabsList variant="line">
            <TabsTrigger value="info">데이터 정보</TabsTrigger>
            <TabsTrigger value="pipeline">파이프라인</TabsTrigger>
            <TabsTrigger value="result">분석 결과</TabsTrigger>
          </TabsList>
          <TabsContent value="info">
            <DataInfoTab {...detail} />
          </TabsContent>
           <TabsContent value="pipeline">
            <PiplineTab detail={detail} />
          </TabsContent>
          <TabsContent value="result">
            <AnalysisResultTab detail={detail} />
          </TabsContent>
        </Tabs>
      </ItemContent>
    </Item>
  );
}
