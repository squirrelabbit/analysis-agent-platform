import { Button } from "../ui/button";
import {
  Dialog,
  DialogClose,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "../ui/dialog";

type Props = {
  btnText: string;
  title?: string;
  onClick?: () => void;
  children: React.ReactNode;
};

export default function DialogForm({
  btnText,
  title,
  onClick,
  children,
}: Props) {
  return (
    <Dialog>
      <form>
        <DialogTrigger asChild>
          <Button>{btnText}</Button>
        </DialogTrigger>
        <DialogContent className="sm:max-w-sm">
          <DialogHeader>
            <DialogTitle>{title || btnText}</DialogTitle>
          </DialogHeader>
          {children}
          <DialogFooter>
            <Button type="submit" onClick={onClick}>
              등록
            </Button>
            <DialogClose asChild>
              <Button variant="outline">닫기</Button>
            </DialogClose>
          </DialogFooter>
        </DialogContent>
      </form>
    </Dialog>
  );
}
