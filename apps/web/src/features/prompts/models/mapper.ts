import type { PromptOptionsResponseDto } from "./dto";
import type { PromptOptions } from "./model";

export const mapPromptOptions = (dto: PromptOptionsResponseDto): PromptOptions => ({
  task: dto.task,
  default: dto.default,
  versions: dto.versions.map((v) => ({
    version: v.version,
    label: v.label || v.version,
  })),
});
