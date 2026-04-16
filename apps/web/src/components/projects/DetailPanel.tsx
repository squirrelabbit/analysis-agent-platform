import type { Project } from "@/types";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../ui/card";
import DetailTabs from "./detail/DetailTabs";

export default function DetailPanel(props: Project) {
  return (
    <Card>
      <CardHeader>
        <CardTitle>{props.name}</CardTitle>
        <CardDescription>{props.description}</CardDescription>
        <div className="flex gap-1 text-xs text-gray-400">
        <div>데이터셋 {props.dataset_version_count} ·</div>
        <div>시나리오 {props.scenario_count} ·</div>
        <div>프롬프트 {props.prompt_count}</div>
        </div>
      </CardHeader>
      <CardContent>
        <DetailTabs {...props} />
      </CardContent>
    </Card>
  );
}
