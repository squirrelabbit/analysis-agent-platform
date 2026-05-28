import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";

export default function MessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";
  return (
    <div className={cn("flex gap-2.5 items-start", isUser && "flex-row-reverse")}>
      <Avatar className="h-7 w-7 shrink-0 mt-0.5">
        <AvatarFallback
          className={cn(
            "text-[10px]",
            isUser
              ? "bg-zinc-200 text-zinc-600"
              : "bg-violet-100 text-violet-700",
          )}
        >
          {isUser ? "나" : "AI"}
        </AvatarFallback>
      </Avatar>
      <div
        className={cn(
          "max-w-[80%] rounded-2xl px-4 py-3 text-sm whitespace-pre-wrap break-words",
          isUser
            ? "bg-violet-600 text-white rounded-tr-sm"
            : "bg-white border border-zinc-100 text-zinc-800 rounded-tl-sm",
        )}
      >
        {message.content}
      </div>
    </div>
  );
}
