import { z } from "zod"

const ymd = /^\d{4}-\d{2}-\d{2}$/

// 축제 기간 1행(연도별). 폼에선 문자열로 다루고 제출 시 숫자로 변환한다.
// before/after_days는 선택(비우면 개방형 = 데이터 전/후 전부).
const festivalPeriodSchema = z.object({
  year: z.string(),
  festival_start: z.string(),
  festival_end: z.string(),
  before_days: z.string().optional(),
  after_days: z.string().optional(),
})

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
    // 축제기간을 하나라도 입력했으면 축제명이 있어야 한다(백엔드 계약: name 필수).
    const hasAnyPeriod = periods.some(
      (p) => p.year || p.festival_start || p.festival_end,
    )
    if (hasAnyPeriod && !val.festivalName?.trim()) {
      ctx.addIssue({
        code: z.ZodIssueCode.custom,
        message: "축제 기간을 입력하려면 축제명이 필요합니다",
        path: ["festivalName"],
      })
    }
    periods.forEach((p, i) => {
      const filled = p.year || p.festival_start || p.festival_end
      if (!filled) return
      if (!/^\d{4}$/.test(p.year.trim())) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "연도(YYYY)", path: ["periods", i, "year"] })
      }
      if (!ymd.test(p.festival_start)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "시작일 필요", path: ["periods", i, "festival_start"] })
      }
      if (!ymd.test(p.festival_end)) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "종료일 필요", path: ["periods", i, "festival_end"] })
      }
      if (ymd.test(p.festival_start) && ymd.test(p.festival_end) && p.festival_end < p.festival_start) {
        ctx.addIssue({ code: z.ZodIssueCode.custom, message: "종료일이 시작일보다 빠릅니다", path: ["periods", i, "festival_end"] })
      }
    })
  })

export type ProjectFormValues = z.infer<typeof projectSchema>
