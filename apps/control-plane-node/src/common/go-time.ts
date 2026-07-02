/**
 * Go control-plane과 byte-parity를 맞추기 위한 시간 포맷 헬퍼.
 *
 * Go는 응답 직전 displaytime.NormalizeForJSON이 **모든 time.Time을 KST(Asia/Seoul,
 * +09:00)로 변환**한 뒤 RFC3339Nano(소수부 trailing zero 제거)로 marshal한다.
 * 즉 응답 timestamp 계약은 항상 `+09:00` 표기다.
 *
 * JS Date는 ms 정밀도라 pg timestamptz의 µs가 유실된다 — db.module에서 OID 1184
 * 파서를 raw text로 바꾸고, 여기서 문자열 연산으로 KST 변환한다(초·소수부는 offset
 * 이동에 불변이므로 그대로 보존).
 */

const KST_OFFSET_MINUTES = 9 * 60;

/** pg timestamptz text ('2026-04-14 13:24:06.995016+09') → Go KST RFC3339. */
export function goTimestamptz(pgText: string): string {
  const m = pgText.match(
    /^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?([+-]\d{2}(?::?\d{2})?)$/,
  );
  if (!m) {
    return pgText;
  }
  return formatKst(m);
}

/**
 * RFC3339 문자열(진행률 파일 updated_at 등) → Go KST RFC3339.
 * Go loadBuildJobProgress는 time.Parse(RFC3339Nano) 실패 시 필드를 생략한다 → null.
 */
export function goRfc3339(input: string): string | null {
  const m = input
    .trim()
    .match(/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$/);
  if (!m) {
    return null;
  }
  return formatKst(m);
}

/** match 그룹 [_, Y, M, D, h, m, s, frac?, offset] → KST 변환 + RFC3339Nano 표기. */
function formatKst(m: RegExpMatchArray): string {
  const [, year, month, day, hour, minute, second, fracRaw, offsetRaw] = m;
  // offset은 분 단위 정수 — 초·소수부는 변환에 영향이 없어 그대로 보존한다.
  const utcMs =
    Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hour), Number(minute)) -
    parseOffsetMinutes(offsetRaw) * 60_000;
  const kst = new Date(utcMs + KST_OFFSET_MINUTES * 60_000);
  const pad = (n: number) => String(n).padStart(2, '0');
  const frac = (fracRaw ?? '').replace(/0+$/, '');
  return (
    `${kst.getUTCFullYear()}-${pad(kst.getUTCMonth() + 1)}-${pad(kst.getUTCDate())}` +
    `T${pad(kst.getUTCHours())}:${pad(kst.getUTCMinutes())}:${second}` +
    `${frac ? '.' + frac : ''}+09:00`
  );
}

/** 'Z' | '+09' | '+09:30' | '+0930' → 분. */
function parseOffsetMinutes(offset: string): number {
  if (offset === 'Z') {
    return 0;
  }
  const sign = offset[0] === '-' ? -1 : 1;
  const digits = offset.slice(1).replace(':', '');
  const hours = Number(digits.slice(0, 2));
  const minutes = digits.length >= 4 ? Number(digits.slice(2, 4)) : 0;
  return sign * (hours * 60 + minutes);
}
