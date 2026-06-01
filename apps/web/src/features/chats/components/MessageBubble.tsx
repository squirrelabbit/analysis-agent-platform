import { Avatar, AvatarFallback } from "@/components/ui/avatar";
import { cn } from "@/lib/utils";
import type { ChatMessage } from "../models";
import DisplayTable from "./DisplayTable";
import PlanPanel from "./PlanPanel";

export default function MessageBubble({ message }: { message: ChatMessage }) {
  const isUser = message.role === "user";
  const hasTable = !isUser && message.display?.type === "table";
  const hasPlan = !isUser && !!message.plan;
  const isWide = hasTable || hasPlan;
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
          "rounded-2xl px-4 py-3 text-sm",
          isWide ? "max-w-full flex-1 min-w-0" : "max-w-[80%]",
          isUser
            ? "bg-violet-600 text-white rounded-tr-sm"
            : "bg-white border border-zinc-100 text-zinc-800 rounded-tl-sm",
        )}
      >
        <div className="whitespace-pre-wrap break-words">{message.content}</div>
        {hasTable && <DisplayTable display={message.display!} />}
        {hasPlan && <PlanPanel plan={message.plan!} />}
      </div>
    </div>
  );
}
