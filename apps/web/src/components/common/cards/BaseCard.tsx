import { Card } from "@/components/ui/card";

export default function BaseCard({
  children,
  ...props
}: React.ComponentProps<"div">) {
  return (
    <Card
      {...props}
      className="group bg-white hover:cursor-pointer hover:-translate-y-0.5 hover:border-[0.5px] hover:border-violet-500 hover:bg-violet-50 hover:shadow-2xl"
    >
      {children}
    </Card>
  );
}
