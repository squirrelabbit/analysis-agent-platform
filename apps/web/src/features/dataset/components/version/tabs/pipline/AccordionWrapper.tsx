import {
  Accordion,
  AccordionContent,
  AccordionItem,
  AccordionTrigger,
} from "@/components/ui/accordion";
import { Alert, AlertDescription } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Empty, EmptyHeader } from "@/components/ui/empty";
import { Play, TriangleAlert } from "lucide-react";
import { SampleCostCard } from "./SampleCostCard";
import { SampleRowPreview } from "./SampleRowPreview";

export function AccordionWrapper({
  children,
  isExcute,
}: {
  children: React.ReactNode;
  isExcute: boolean;
}) {
  return (
    <Accordion type="single" collapsible className="border-t">
      <AccordionItem value="sample">
        <AccordionTrigger className="gap-3">
          <p>샘플 테스트 - 실행전 비용 확인</p>
          <Badge>{isExcute ? "완료됨" : "미실행"}</Badge>
        </AccordionTrigger>
        <AccordionContent>
          {children}
          {!isExcute && (
            <Empty>
              <EmptyHeader>
                <p>샘플 실행 내역 없음</p>
                <Button>샘플 실행</Button>
              </EmptyHeader>
              샘플을 먼저 실행해 비용과 결과 품질을 확인하세요.
            </Empty>
          )}
        </AccordionContent>
      </AccordionItem>
    </Accordion>
  );
}

export function PrepareAccordion() {
  return (
    <AccordionWrapper isExcute={true}>
      <div className="border border-border rounded-lg overflow-hidden">
        {/* 헤더 */}
        <div
          className="flex items-center justify-between px-3 py-2.5
                      bg-muted/40 border-b border-border"
        >
          <span className="text-[12px] font-semibold text-foreground">
            샘플 결과
          </span>
          <div className="flex gap-1.5">
            <Badge
              variant="outline"
              className="text-[10px] border-emerald-200
                       bg-emerald-50 text-emerald-700"
            >
              10건 성공
              {/* {result.successCount.toLocaleString("ko-KR")}건 성공 */}
            </Badge>
            {/* {result.skipCount > 0 && ( */}
            <Badge
              variant="outline"
              className="text-[10px] border-red-200 bg-red-50 text-red-600"
            >
              2건 skip
            </Badge>
            {/* )} */}
          </div>
        </div>

        <div className="p-3 space-y-3">
          {/* 비용 */}
          <SampleCostCard
            cost={{
              sampleCost: 0.0031,
              totalEstimated: 0.2963,
              costPerRow: 0.000006,
              estimatedMinutes: 54,
              totalRows: 5,
            }}
            sampleSize={5}
          />

          {/* 미리보기 */}
          <div>
            <p
              className="text-[10px] font-mono text-muted-foreground
                        uppercase tracking-wide mb-2"
            >
              처리 결과 미리보기
            </p>
            <SampleRowPreview
              rows={[
                {
                  index: 1,
                  before:
                    "진짜 너무 좋았어요!! ㅋㅋ 완전 대박🎉🎉 www.test.com",
                  after: "진짜 너무 좋았어요 완전 대박",
                  skipped: false,
                },
                {
                  index: 1,
                  before: "ㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋㅋ",
                  after: "(의미없는 반복)",
                  skipped: true,
                },
              ]}
            />
          </div>

          {/* 전체 실행 경고 + 액션 */}
          <Alert className="border-amber-200 bg-amber-50 py-2.5 px-3">
            <TriangleAlert className="h-3.5 w-3.5 text-amber-600" />
            <AlertDescription className="text-[11px] text-amber-700">
              <p>
                전체 실행 시 약 <strong>$0.30</strong>의 LLM 비용이 발생합니다.
              </p>
              <div className="flex gap-2 mt-2">
                <Button size="sm" variant="outline" className="h-7 text-[11px]">
                  취소
                </Button>
                <Button size="sm" variant="outline">
                  <Play className="w-2.5 h-2.5" />
                  확인 후 전체 실행
                </Button>
              </div>
            </AlertDescription>
          </Alert>
        </div>
      </div>
    </AccordionWrapper>
  );
}

export function SentimentAccordion() {
  return (
    <AccordionWrapper isExcute={false}>
      <Button>파일 업로드</Button>
    </AccordionWrapper>
  );
}
