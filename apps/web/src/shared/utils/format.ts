export function fmtDate(iso: string): string {
  if (!iso) return "—";
  return iso.slice(0, 10);
}

export function formatFileSize(size: number) {
  if (size < 1024) return size + " B"
  if (size < 1024 * 1024) return (size / 1024).toFixed(1) + " KB"
  if (size < 1024 * 1024 * 1024) return (size / 1024 / 1024).toFixed(1) + " MB"
  return (size / 1024 / 1024 / 1024).toFixed(1) + " GB"
}
