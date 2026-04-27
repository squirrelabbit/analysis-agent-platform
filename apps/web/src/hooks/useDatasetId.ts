import { useParams } from "react-router-dom";

export function useDatasetId() {
  const { datasetId } = useParams();
  if (!datasetId) {
    throw new Error("datasetId 없음");
  }

  return {
    datasetId
  };
}
