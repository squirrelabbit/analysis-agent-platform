import { Item, ItemContent, ItemHeader } from "@/components/ui/item";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";

export default function ProjectItemSkeleton({
  view,
}: {
  view: "grid" | "list";
}) {
  return view === "grid" ? <ProjectGridSkeleton /> : <ProjectListSkeleton />;
}

function ProjectListSkeleton() {
  return (
    <div className="flex flex-col gap-3">
      {Array.from({ length: 6 }).map((_, idx) => (
        <Item key={idx} className="animate-pulse bg-white">
          <ItemHeader>
            <div className="flex-1 space-y-2">
              <Skeleton className="h-4 w-40" />
              <Skeleton className="h-3 w-64" />
            </div>
            <div className="flex gap-2">
              {Array.from({ length: 3 }).map((_, i) => (
                <Skeleton key={i} className="h-6 w-16 rounded-full" />
              ))}
            </div>
          </ItemHeader>
        </Item>
      ))}
    </div>
  );
}

export function ProjectGridSkeleton() {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
      {Array.from({ length: 6 }).map((_, idx) => (
        <Item key={idx} className="bg-white">
          <ItemHeader>
            <Skeleton className="h-10 w-10 rounded-md" />

            <ItemContent className="pl-2">
              <ItemHeader>
                <div className="space-y-2">
                  <Skeleton className="h-4 w-32 rounded-md" />
                  <Skeleton className="h-3 w-52 rounded-md" />
                </div>
              </ItemHeader>

              <div className="flex items-center gap-1 pt-2">
                <Skeleton className="h-3 w-3 rounded-full" />
                <Skeleton className="h-3 w-24 rounded-md" />
              </div>
            </ItemContent>
          </ItemHeader>

          <Separator className="my-1.5" />

          <div className="flex gap-2">
            {Array.from({ length: 3 }).map((_, i) => (
              <Skeleton key={i} className="h-5 w-18 rounded-full" />
            ))}
          </div>
        </Item>
      ))}
    </div>
  );
}
