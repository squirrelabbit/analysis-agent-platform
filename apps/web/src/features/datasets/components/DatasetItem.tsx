import BaseCard from "@/components/common/cards/BaseCard";
import type { Dataset } from "../models/model";
import {
  CardAction,
  CardDescription,
  CardFooter,
  CardHeader,
  CardTitle,
} from "@/components/ui/card";
import { ChevronRight } from "lucide-react";
import { Badge } from "@/components/ui/badge";
import { useNavigate } from "react-router-dom";
import DeleteDialog from "@/components/common/dialogs/DeleteDialog";
import { useDeleteDataset } from "../hooks/dataset.mutation";
import MetadataDialog from "./MetadataDialog";

export default function DatasetItem(dataset: Dataset) {
  const navigate = useNavigate();
  const { mutateAsync: onDelete } = useDeleteDataset();
  const { id, name, description, dataType, activeDatasetVersionId } = dataset;

  return (
    <BaseCard className="group" onClick={() => navigate(id)}>
      <CardHeader className="flex-1">
        <CardTitle>{name}</CardTitle>
        <CardDescription>{description}</CardDescription>
        <CardAction
          className="flex items-center text-zinc-400 opacity-0 transition-opacity group-hover:opacity-100"
          onClick={(e) => e.stopPropagation()}
        >
          <MetadataDialog datasetId={id} />
          <DeleteDialog title="데이터셋" onDelete={() => onDelete(id)}>
            <div>데이터셋명: {name}</div>
          </DeleteDialog>
          <ChevronRight className="w-4 h-4 group-hover:text-violet-600" />
        </CardAction>
      </CardHeader>
      <CardFooter className="justify-between">
        <Badge variant="outline" className="text-xs">
          {dataType === "structured" ? "정형" : "비정형"}
        </Badge>
        <Badge variant="outline" className="text-xs">
          {activeDatasetVersionId ? "활성" : "비활성"}
        </Badge>
      </CardFooter>
    </BaseCard>
  );
}
