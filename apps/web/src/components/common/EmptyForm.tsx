import type { JSX } from "react";
import {
  Empty,
  EmptyContent,
  EmptyDescription,
  EmptyHeader,
  EmptyMedia,
  EmptyTitle,
} from "../ui/empty";

type EmptyFormProps = {
  title: string;
  description: string;
  icon: JSX.Element;
  children?: React.ReactNode;
  props?: React.ComponentProps<"div">
};

export function EmptyForm({
  title,
  description,
  icon,
  children,
  props
}: EmptyFormProps) {
  return (
    <Empty {...props}>
      <EmptyHeader>
        <EmptyMedia variant="icon">{icon}</EmptyMedia>
        <EmptyTitle>{title}</EmptyTitle>
        <EmptyDescription className="text-xs">{description}</EmptyDescription>
      </EmptyHeader>
      <EmptyContent>{children}</EmptyContent>
    </Empty>
  );
}
