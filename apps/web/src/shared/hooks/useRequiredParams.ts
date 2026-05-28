import { useParams } from "react-router-dom";

export function useRequiredParams<T extends Record<string, string>>(
  keys: (keyof T)[],
): T {
  const params = useParams();

  for (const key of keys) {
    if (!params[key as string]) {
      throw new Error(`Missing route param: ${String(key)}`);
    }
  }

  return params as T;
}
