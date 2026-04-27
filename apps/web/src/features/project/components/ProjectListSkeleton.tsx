import { Skeleton } from "@/components/ui/skeleton";

export default function ProjectListSkeleton () {
  return (
    // <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
    <div className="flex flex-col gap-2">
      {Array.from({ length: 6 }).map((_, index) => (
        <div className="rounded-[10px] border border-[#e2e5ed] bg-white p-5" key={index}>
          <Skeleton className="mb-3 h-4 w-2/3" />
          <Skeleton className="mb-2 h-3 w-full" />
          <Skeleton className="mb-4 h-3 w-4/5" />
          <Skeleton className="h-3 w-1/2" />
        </div>
      ))}
    </div>
  );
};