import type {
  AnalysisMessageDto,
  AnalysisThreadDetailDto,
  AnalysisThreadDto,
  AnalysisThreadMessageResponseDto,
  ComposerDisplayDto,
} from "./dto";
import type {
  AnalyzeResult,
  ChatDisplay,
  ChatMessage,
  ChatThread,
  ChatThreadDetail,
} from "./model";

const mapDisplay = (dto: ComposerDisplayDto | undefined): ChatDisplay | undefined => {
  if (!dto || dto.type !== "table") return undefined;
  const columns = dto.columns ?? [];
  const rows = dto.rows ?? [];
  if (columns.length === 0) return undefined;
  return {
    type: "table",
    title: dto.title ?? undefined,
    columns,
    rows,
  };
};

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
          display: mapDisplay(dto.result?.composer?.display),
        }
      : undefined;

  const userMessage: ChatMessage = {
    id: dto.user_message.message_id,
    role: "user",
    content: dto.user_message.content,
    createdAt: dto.user_message.created_at,
  };

  return {
    threadId: dto.thread_id,
    userMessage,
    assistantMessage,
    errorMessage: dto.run?.error_message ?? null,
  };
};

export const mapThread = (dto: AnalysisThreadDto): ChatThread => ({
  id: dto.thread_id,
  title: dto.title?.trim() || "제목 없음",
  lastMessage: dto.last_message ?? "",
  messageCount: dto.message_count ?? 0,
  updatedAt: dto.updated_at,
});

// thread detail의 assistant messages에는 composer.display projection이
// 동봉되므로 POST 응답과 동일하게 mapDisplay로 매핑한다.
const mapStoredMessage = (dto: AnalysisMessageDto): ChatMessage => ({
  id: dto.message_id,
  role: dto.role,
  content: dto.content,
  createdAt: dto.created_at,
  display: mapDisplay(dto.display),
});

export const mapThreadDetail = (dto: AnalysisThreadDetailDto): ChatThreadDetail => ({
  id: dto.thread_id,
  messages: dto.messages.map(mapStoredMessage),
});
