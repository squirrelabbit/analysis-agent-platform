export const projectKeys = {
  all: ['projects'] as const,

  lists: () => [...projectKeys.all, 'list'] as const, // 실제 query key

  details: () => [...projectKeys.all, 'detail'] as const, // detail 계열 invalidate용 그룹 key
  detail: (id: string) =>
    [...projectKeys.details(), id] as const,  //실제 detail query key
}