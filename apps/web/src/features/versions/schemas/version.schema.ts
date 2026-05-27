import z from "zod";

export const versionSchema = z.object({
  file: z.instanceof(File, { message: "파일을 선택하세요" }),
  dataType: z.enum(["structured", "unstructured"], {
    error: "데이터 타입을 선택하세요",
  }),
  activateOnCreate: z.boolean(),
});

export type VersionFormValues = z.infer<typeof versionSchema>;
