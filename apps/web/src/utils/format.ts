export function fmtDate(iso: string): string {
  if (!iso) return "—";
  return iso.slice(0, 10);
}