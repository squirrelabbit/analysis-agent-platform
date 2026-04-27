import type { DatasetFormValues } from "../schcema/dataset.schcema";
import type { Dataset } from "../types/dataset";
import type {
  CreateDatasetRequest,
  DatasetResponse,
} from "../types/dataset.dto";

export const mapDataset = (dto: DatasetResponse): Dataset => ({
  id: dto.dataset_id,
  projectId: dto.project_id,
  name: dto.name,
  description: dto.description,
  dataType: dto.data_type,
  activeDatasetVersionId: dto.active_dataset_version_id,
  activeVersionUpdatedAt: dto.active_version_updated_at,
  createdAt: dto.created_at
});

export const mapDatasetFormToRequest = (
  form: DatasetFormValues,
): CreateDatasetRequest => ({
  name: form.name,
  description: form.description,
  data_type: form.dataType,
});


