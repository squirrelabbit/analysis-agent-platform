export const chatKeys = {
  all: ["chats"] as const,

  threads: () => [...chatKeys.all, "threads"] as const,
  threadList: (projectId: string, datasetId: string) =>
    [...chatKeys.threads(), "list", projectId, datasetId] as const,
  threadDetail: (projectId: string, datasetId: string, threadId: string) =>
    [...chatKeys.threads(), "detail", projectId, datasetId, threadId] as const,
};
