import { useQuery } from "@tanstack/react-query";
import { chatApi } from "../api/chat.api";
import { chatKeys } from "../api/chat.key";
import { mapThread, mapThreadDetail } from "../models";

export const useChatThreads = (projectId: string, datasetId: string) =>
  useQuery({
    queryKey: chatKeys.threadList(projectId, datasetId),
    queryFn: () => chatApi.listThreads(projectId, datasetId),
    enabled: !!projectId && !!datasetId,
    select: (data) => data.map(mapThread),
  });

export const useChatThread = (
  projectId: string,
  datasetId: string,
  threadId: string | null,
) =>
  useQuery({
    queryKey: chatKeys.threadDetail(projectId, datasetId, threadId ?? ""),
    queryFn: () => chatApi.getThread(projectId, datasetId, threadId!),
    enabled: !!projectId && !!datasetId && !!threadId,
    select: mapThreadDetail,
  });
