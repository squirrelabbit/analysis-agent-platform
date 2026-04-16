import { useDataset } from "@/hooks/useDataset";
import type { Project } from "@/types";
import { Database, Plus } from "lucide-react";
import { useState } from "react";
import { CreateDatasetDialog } from "../dataset/CreateDatasetDialog";
import { EmptyForm } from "@/components/common/EmptyForm";
import { DatasetItem } from "../dataset/DatasetItem";
import { Button } from "@/components/ui/button";

export default function DatasetTab({ project_id }: Project) {
  const { datasets, addDataset, findDatasetById, dataset } =
    useDataset(project_id);
  const [createOpen, setCreateOpen] = useState(false);

  /* 데이터셋 등록 */
  const handleCreate = (
    name: string,
    description: string,
    data_type: string,
  ) => {
    addDataset({ name, description, data_type });
  };

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between gap-3">
        <div>
          <h3 className="text-sm font-semibold text-zinc-800">데이터셋</h3>
          <p className="text-xs text-zinc-500 mt-0.5">
            {datasets.length}개 데이터셋
          </p>
        </div>
        <Button
          size="sm"
          onClick={() => setCreateOpen(true)}
          className="h-8 text-xs"
        >
          <Plus className="w-3.5 h-3.5" />
          데이터셋 등록
        </Button>
      </div>

      {/* 목록 */}
      {datasets.length === 0 ? (
        <EmptyForm
          title="등록된 데이터셋이 없습니다"
          description="데이터셋을 먼저 등록한 뒤 파일을 업로드하세요"
          icon={<Database className="text-zinc-400" />}
        />
      ) : (
        datasets.map((ds) => (
          <DatasetItem
            key={ds.dataset_id}
            dataset={ds}
            findDataset={findDatasetById}
            selected={dataset}
          />
        ))
      )}

      {/* Dialogs */}
      <CreateDatasetDialog
        open={createOpen}
        onClose={() => setCreateOpen(false)}
        onCreate={handleCreate}
      />
    </div>
  );
}
