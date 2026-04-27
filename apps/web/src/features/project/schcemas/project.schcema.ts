import { z } from "zod"

export const projectSchema = z.object({
  name: z.string().min(1, "이름은 필수입니다"),
  description: z.string().min(1, "설명은 필수입니다"),
})

export type ProjectFormValues = z.infer<typeof projectSchema>