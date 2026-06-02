import { useQuery } from "@tanstack/react-query";
import { promptApi } from "../api/prompt.api";
import { promptKeys } from "../api/prompt.key";
import { mapPromptOptions, type PromptOptionsTask } from "../models";

// prompt 카탈로그는 서버 파일이 추가될 때까지 거의 변하지 않으므로 stale을
// 길게 두어 화면 전환마다 재요청하지 않는다.
const STALE_MS = 5 * 60 * 1000;

export const usePromptOptions = (task: PromptOptionsTask) =>
  useQuery({
    queryKey: promptKeys.options(task),
    queryFn: () => promptApi.getOptions(task),
    select: mapPromptOptions,
    staleTime: STALE_MS,
  });
