import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { datasetKeys } from "../constants/queryKeys";
import { datasetsApi } from "../api/dataset.api";
import { mapDataset } from "../api/dataset.mapper";
import type { CreateDatasetRequest } from "../types/dataset.dto";

export const useDataset = (projectId: string) => {
  const queryClient = useQueryClient();

  const query = useQuery({
    queryKey: datasetKeys.lists(),
    queryFn: async () => {
      const data = await datasetsApi.getDatasets(projectId);
      return data.map(mapDataset);
    },
  });

  const create = useMutation({
    mutationFn: ({
      projectId,
      req,
    }: {
      projectId: string;
      req: CreateDatasetRequest;
    }) => datasetsApi.createDataset(projectId, req),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["datasets"] });
    },
  });

  const remove = useMutation({
    mutationFn: datasetsApi.deleteDataset,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["datasets"] });
    },
  });

  return {
    ...query,
    create,
    remove,
  };
};
