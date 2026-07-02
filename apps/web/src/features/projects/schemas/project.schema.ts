import { z } from "zod"

const ymd = /^\d{4}-\d{2}-\d{2}$/

// 축제 기간 1행(연도별). 폼에선 문자열로 다루고 제출 시 숫자로 변환한다(2026-07-02 재설계).
// 대상기간(target)·축제기간(festival)을 각각 날짜 범위로 입력하고, 역할(기준/비교)을 지정한다.
const festivalPeriodSchema = z.object({
  year: z.string(),
  role: z.enum(["base", "compare"]),
  target_start: z.string(),
  target_end: z.string(),
  festival_start: z.string(),
  festival_end: z.string(),
})

// 이 행에 뭔가 입력됐는지(부분 입력 검증 대상 판별).
const periodFilled = (p: z.infer<typeof festivalPeriodSchema>) =>
  !!(p.year || p.target_start || p.target_end || p.festival_start || p.festival_end)

export const projectSchema = z
  .object({
    name: z.string().min(1, "이름은 필수입니다"),
    description: z.string().min(1, "설명은 필수입니다"),
    // 축제 메타(선택). 이름을 넣으면 축제 정보로 저장된다.
    festivalName: z.string().optional(),
    periods: z.array(festivalPeriodSchema).optional(),
  })
  .superRefine((val, ctx) => {
    const periods = val.periods ?? []
    const active = periods.filter(periodFilled)
    // 축제기간을 하나라도 입력했으면 축제명이 있어야 한다(백엔드 계약: name 필수).
    if (active.length > 0 && !val.festivalName?.trim()) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "축제 기간을 입력하려면 축제명이 필요합니다",
        path: ["festivalName"],
      })
    }
    let baseCount = 0
    periods.forEach((p, i) => {
      if (!periodFilled(p)) return
      if (p.role === "base") baseCount++
      const year = p.year.trim()
      if (!/^\d{4}$/.test(year)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "연도(YYYY)", path: ["periods", i, "year"] })
      }
      // 대상 기간
      if (!ymd.test(p.target_start)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "대상 시작일 필요", path: ["periods", i, "target_start"] })
      }
      if (!ymd.test(p.target_end)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "대상 종료일 필요", path: ["periods", i, "target_end"] })
      }
      if (ymd.test(p.target_start) && ymd.test(p.target_end) && p.target_end < p.target_start) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "대상 종료일이 시작일보다 빠릅니다", path: ["periods", i, "target_end"] })
      }
      // 축제 기간
      if (!ymd.test(p.festival_start)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "축제 시작일 필요", path: ["periods", i, "festival_start"] })
      }
      if (!ymd.test(p.festival_end)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "축제 종료일 필요", path: ["periods", i, "festival_end"] })
      }
      if (ymd.test(p.festival_start) && ymd.test(p.festival_end) && p.festival_end < p.festival_start) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "축제 종료일이 시작일보다 빠릅니다", path: ["periods", i, "festival_end"] })
      }
      // 축제기간 ⊆ 대상기간
      if (ymd.test(p.target_start) && ymd.test(p.festival_start) && p.festival_start < p.target_start) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "축제 시작일은 대상 시작일 이후여야 합니다", path: ["periods", i, "festival_start"] })
      }
      if (ymd.test(p.target_end) && ymd.test(p.festival_end) && p.festival_end > p.target_end) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "축제 종료일은 대상 종료일 이전이어야 합니다", path: ["periods", i, "festival_end"] })
      }
      // 축제 시작일 연도 == year
      if (/^\d{4}$/.test(year) && ymd.test(p.festival_start) && p.festival_start.slice(0, 4) !== year) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "축제 시작일 연도가 연도와 다릅니다", path: ["periods", i, "festival_start"] })
      }
    })
    if (active.length > 0 && baseCount > 1) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "기준 연도는 1개만 지정할 수 있습니다",
        path: ["festivalName"],
      })
    }
  })

export type ProjectFormValues = z.infer<typeof projectSchema>
