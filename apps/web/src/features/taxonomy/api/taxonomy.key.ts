export const taxonomyKeys = {
  all: ["taxonomy"] as const,
  // taxonomyId 미지정은 default 슬롯("default")로 캐싱.
  detail: (taxonomyId?: string) =>
    [...taxonomyKeys.all, "detail", taxonomyId ?? "default"] as const,
  list: () => [...taxonomyKeys.all, "list"] as const,
};
