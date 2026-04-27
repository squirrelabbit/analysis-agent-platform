import { Card } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";

export function VersionSkeleton() {
  return (
    <Card className="flex flex-row p-4">
      <div className="w-70">
        <Skeleton className="mb-3 h-8 w-2/3" />
        {Array.from({ length: 2 }).map((_, index) => (
          <div
            className="gap-4 rounded-[10px] border border-[#e2e5ed] p-4 mb-3"
            key={index}
          >
            <Skeleton className="mb-3 h-4 w-2/3" />
            <Skeleton className="mb-2 h-3 w-full" />
          </div>
        ))}
      </div>
      <div className="flex-1">
        <VersionDetailSkeleton />
      </div>
    </Card>
  );
}

export function VersionDetailSkeleton() {
  return (
    <div>
      <Skeleton className="h-4 w-1/3" />
      <div className="flex gap-3 my-6">
        {Array.from({ length: 3 }).map((_, index) => (
          <Skeleton key={index} className="mb-3 h-15 w-1/3" />
        ))}
      </div>
      <Skeleton className="mb-3 h-4 w-full" />
      <Skeleton className="mb-3 h-4 w-full" />
    </div>
  );
}
