import type { Dataset } from "../types/dataset";
import type {
  CreateDatasetRequest,
  DatasetResponse,
} from "../types/dataset.dto";
import type { DatasetForm } from "../types/dataset.form";

export const mapDataset = (dto: DatasetResponse): Dataset => ({
  id: dto.dataset_id,
  projectId: dto.project_id,
  name: dto.name,
  description: dto.description,
  dataType: dto.data_type,
  activeDatasetVersionId: dto.active_dataset_version_id,
});

export const mapDatasetFormToRequest = (
  form: DatasetForm,
): CreateDatasetRequest => ({
  name: form.name,
  description: form.description,
  data_type: form.dataType, // camel → snake 변환
});
