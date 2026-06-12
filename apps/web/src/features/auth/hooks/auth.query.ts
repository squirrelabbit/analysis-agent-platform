import { useQuery } from "@tanstack/react-query";
import { authApi } from "../api/auth.api";

export const authKeys = {
  me: ["auth", "me"] as const,
  config: ["auth", "config"] as const,
};

// /auth/me — 미로그인은 401(정상 상태)이므로 재시도하지 않는다.
// 호출부에서 isError를 "미로그인"으로 다룬다.
export const useAuthMe = () =>
  useQuery({
    queryKey: authKeys.me,
    queryFn: authApi.getMe,
    retry: false,
    staleTime: 5 * 60_000,
  });

export const useAuthConfig = () =>
  useQuery({
    queryKey: authKeys.config,
    queryFn: authApi.getConfig,
    staleTime: Infinity,
  });
