export function fmtDate(iso: string): string {
  if (!iso) return "—";
  return iso.slice(0, 10);
}

export function formatFileSize(size: number) {
  if (size < 1024) return size + " B";
  if (size < 1024 * 1024) return (size / 1024).toFixed(1) + " KB";
  if (size < 1024 * 1024 * 1024) return (size / 1024 / 1024).toFixed(1) + " MB";
  return (size / 1024 / 1024 / 1024).toFixed(1) + " GB";
}

export function formatSecond(durationSeconds: number | undefined) {
  const totalSec = Math.round(durationSeconds ?? 0);
  return totalSec >= 60
    ? `${Math.floor(totalSec / 60)}분 ${totalSec % 60}초`
    : `${totalSec}초`;
}
