import { Badge } from "@/components/ui/badge";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Separator } from "@/components/ui/separator";
import type { Prompt } from "@/types";

export function PromptCard (prompt: Prompt) {
  return (
    <Card>
        <CardHeader>
          <CardTitle>{prompt.title}</CardTitle>
          <CardDescription>{prompt.version}</CardDescription>
          <CardAction>
          <Badge variant="secondary">{prompt.status}</Badge>
        </CardAction>
        </CardHeader>
        <Separator />
        <CardContent>
          <CardDescription>{prompt.summary}</CardDescription>
          {prompt.content}
          {/* <div>
            You are preparing raw VOC or issue text for deterministic downstream analysis.
            <br />
            - Keep the original meaning.<br />
            - Remove only obvious noise, duplicated punctuation, and boilerplate.<br />
            - Do not summarize beyond a short normalization.<br />
            - Do not invent facts.<br />
            - Choose disposition `keep`, `review`, or `drop`.<br />
            - Use `drop` only for empty, unreadable noise, or clear non-content rows.<br />
            - Use `review` when the text is partially readable but low quality or mixed.<br />

          </div> */}
        </CardContent>
      </Card>
  )
}