import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../ui/card";
import DetailTabs from "./detail/DetailTabs";

export default function DetailPanel({ project }: { project: any }) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{project.name}</CardTitle>
        <CardDescription>{project.description}</CardDescription>
        <div className="flex gap-1 text-xs text-gray-400">
        <div>데이터셋 ·</div>
        <div>시나리오 ·</div>
        <div>프롬프트</div>
        </div>
      </CardHeader>
      <CardContent>
        <DetailTabs />
      </CardContent>
    </Card>
  );
}
