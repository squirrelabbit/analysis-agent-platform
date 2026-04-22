import { useDataset } from "../hooks/useDataset";
import DatasetList from "./DatasetList";
import CreateDialog from "@/components/common/dialogs/CreateDialog";
import CreateDatasetForm from "./CreateDatasetForm";
import { useState } from "react";
import type { DatasetForm } from "../types/dataset.form";
import { mapDatasetFormToRequest } from "../api/dataset.mapper";

export default function DatasetTab({ projectId }: { projectId: string }) {
  const { data: datasets = [], create } = useDataset(projectId);
  const [payload, setPayload] = useState<DatasetForm>({
    name: "",
    description: "",
    dataType: "unstructured",
  });
  return (
    <div className="">
      <div className="flex justify-between items-center pb-2">
        <p className="text-xs text-zinc-500">전체 {datasets.length}개</p>
        <CreateDialog
          title="데이터셋"
          onSave={() =>
            create.mutate({ projectId, req: mapDatasetFormToRequest(payload) })
          }
        >
          <CreateDatasetForm
            onChange={(k, v) => setPayload({ ...payload, [k]: v })}
          />
        </CreateDialog>
      </div>
      <DatasetList datasets={datasets} />
    </div>
  );
}
