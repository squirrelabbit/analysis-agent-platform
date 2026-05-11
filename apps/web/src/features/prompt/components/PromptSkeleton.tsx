import { Skeleton } from "@/components/ui/skeleton";
import { cn } from "@/lib/utils";
import { LoaderIcon } from "lucide-react";

export function PromptListSkeleton() {
  return (
    <div>
      <Skeleton className="h-8 w-full my-3" />

      <div className="flex flex-col gap-6">
        {Array.from({ length: 3 }).map((_, index) => (
          <div key={index} className="flex flex-col gap-3">
            <Skeleton className="h-4 w-20" />
            <Skeleton className="h-8 w-full" />
          </div>
        ))}
      </div>
    </div>
  );
}

export function PromptDetailSkeleton() {
  return (
    <div className="h-1/2 flex items-center justify-center">
      <LoaderIcon
        role="status"
        aria-label="Loading"
        className={cn("size-4 animate-spin")}
      />
    </div>
  );
}
