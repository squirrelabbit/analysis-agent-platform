// silverone 2026-06-02 — client-side 임시 id 생성 helper.
// crypto.randomUUID()는 secure context(HTTPS/localhost)에서만 정의되므로,
// http://<ip>:5173 같은 비-secure 배포에서는 undefined → 호출 시 TypeError.
// 화면 내부에서만 쓰는 message id라 fallback chain으로 안전하게 만든다.
//   1) crypto.randomUUID  (secure context)
//   2) crypto.getRandomValues 기반 UUID v4
//   3) Date.now + Math.random (비암호화 최후 fallback)
export function createClientId(): string {
  const c = globalThis.crypto as Crypto | undefined;
  if (c?.randomUUID) {
    return c.randomUUID();
  }
  if (c?.getRandomValues) {
    const bytes = c.getRandomValues(new Uint8Array(16));
    bytes[6] = (bytes[6] & 0x0f) | 0x40; // version 4
    bytes[8] = (bytes[8] & 0x3f) | 0x80; // variant 10xx
    const hex = Array.from(bytes, (b) => b.toString(16).padStart(2, "0")).join("");
    return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
  }
  return `id-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 10)}`;
}
