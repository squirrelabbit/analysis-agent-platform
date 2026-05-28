import type { AnalysisThreadMessageResponseDto } from "./dto";
import type { AnalyzeResult, ChatMessage } from "./model";

export const mapAnalyzeResponse = (
  dto: AnalysisThreadMessageResponseDto,
): AnalyzeResult => {
  // 사용자 명시: assistant 텍스트는 result.composer.assistant_content 기준,
  // 없으면 assistant_message.content로 폴백.
  const content =
    dto.result?.composer?.assistant_content ?? dto.assistant_message?.content;

  const assistantMessage: ChatMessage | undefined =
    content !== undefined && dto.assistant_message
      ? {
          id: dto.assistant_message.message_id,
          role: "assistant",
          content,
          createdAt: dto.assistant_message.created_at,
        }
      : undefined;

  return {
    threadId: dto.thread_id,
    assistantMessage,
    errorMessage: dto.run?.error_message ?? null,
  };
};
