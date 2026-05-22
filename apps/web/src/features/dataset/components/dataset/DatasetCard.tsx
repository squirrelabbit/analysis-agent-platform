import {
  Card,
  CardDescription,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import type { Dataset } from "../../types/dataset";

export default function DatasetCard({ datasets }: { datasets: Dataset[] }) {
  const cards = [
    {
      title: "전체 데이터셋",
      count: datasets.length,
      desc: `활성 버전 ${datasets.filter(({ activeDatasetVersionId }) => !!activeDatasetVersionId).length}개`,
    },
    {
      title: "분석 가능",
      count: datasets.filter(({ dataType }) => dataType === "unstructured")
        .length,
      desc: `비정형 데이터`,
    },
    {
      title: "분석 불가",
      count: datasets.filter(({ dataType }) => dataType === "structured")
        .length,
      desc: `정형 데이터`,
    },
  ];
  return (
    <div className="flex justify-between gap-5">
      {cards.map((item, idx) => (
        <Card key={idx} className="flex-1 shadow-md">
          <CardHeader>
            <CardDescription>{item.title}</CardDescription>
            <CardTitle className="flex text-xl gap-1">
              {item.count}
            </CardTitle>
            <p>{item.desc}</p>
          </CardHeader>
        </Card>
      ))}
    </div>
  );
}
