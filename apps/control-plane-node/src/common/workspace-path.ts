/**
 * metadata에 저장되는 파일 경로(progress_ref, artifact ref 등)는 컨테이너 기준
 * (/workspace/data/...)이다. Node를 host에서 띄우는 동안은 WORKSPACE_DATA_DIR
 * (= compose의 ./data mount에 대응하는 host 경로)로 prefix를 치환한다.
 * 컨테이너 배포에선 미설정 → 경로 그대로 (Go와 동일).
 */
export function rewriteWorkspacePath(path: string): string {
  const hostDir = process.env.WORKSPACE_DATA_DIR;
  if (hostDir && path.startsWith('/workspace/data/')) {
    return hostDir + path.slice('/workspace/data'.length);
  }
  return path;
}
