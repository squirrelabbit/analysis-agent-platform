import { Button } from "@/components/ui/button";
import { ChevronLeft, ChevronRight } from "lucide-react";

interface PaginationControlsProps {
  currentPage: number;
  totalPages: number;
  onPageChange: (page: number) => void;
}

export default function PaginationControls({
  currentPage,
  totalPages,
  onPageChange,
}: PaginationControlsProps) {
  return (
    <div className="place-self-center pt-3">
      <div className="flex items-center gap-2">
        {/* 이전 페이지 버튼 */}
        <Button
          variant="outline"
          onClick={() => onPageChange(currentPage - 1)}
          disabled={currentPage === 1}
          className="gap-1 transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-violet-100 hover:border-violet-300 hover:text-violet-600"
        >
          <ChevronLeft className="w-4 h-4" />
          <span className="hidden sm:inline">이전</span>
        </Button>

        {/* 페이지 표시 */}
        <div className="flex items-center gap-1">
          {/* 첫 페이지 */}
          {currentPage > 2 && (
            <>
              <Button
                variant="ghost"
                onClick={() => onPageChange(1)}
                className="w-8 h-8 p-0 text-slate-600 hover:bg-violet-100 hover:text-violet-600 transition-all duration-300"
              >
                1
              </Button>
              {currentPage > 3 && (
                <span className="text-slate-400 px-1">...</span>
              )}
            </>
          )}

          {/* 이전 페이지 */}
          {currentPage > 1 && (
            <Button
              variant="ghost"
              onClick={() => onPageChange(currentPage - 1)}
              className="w-8 h-8 p-0 text-slate-600 hover:bg-violet-100 hover:text-violet-600 transition-all duration-300"
            >
              {currentPage - 1}
            </Button>
          )}

          {/* 현재 페이지 */}
          <Button
            variant="default"
            disabled
            className="w-8 h-8 p-0 bg-violet-600 text-white font-medium"
          >
            {currentPage}
          </Button>

          {/* 다음 페이지 */}
          {currentPage < totalPages && (
            <Button
              variant="ghost"
              onClick={() => onPageChange(currentPage + 1)}
              className="w-8 h-8 p-0 text-slate-600 hover:bg-violet-100 hover:text-violet-600 transition-all duration-300"
            >
              {currentPage + 1}
            </Button>
          )}

          {/* 마지막 페이지 */}
          {currentPage < totalPages - 1 && (
            <>
              {currentPage < totalPages - 2 && (
                <span className="text-slate-400 px-1">...</span>
              )}
              <Button
                variant="ghost"
                onClick={() => onPageChange(totalPages)}
                className="w-8 h-8 p-0 text-slate-600 hover:bg-violet-100 hover:text-violet-600 transition-all duration-300"
              >
                {totalPages}
              </Button>
            </>
          )}
        </div>

        {/* 다음 페이지 버튼 */}
        <Button
          variant="outline"
          onClick={() => onPageChange(currentPage + 1)}
          disabled={currentPage === totalPages}
          className="gap-1 transition-all duration-300 disabled:opacity-50 disabled:cursor-not-allowed hover:bg-violet-100 hover:border-violet-300 hover:text-violet-600"
        >
          <span className="hidden sm:inline">다음</span>
          <ChevronRight className="w-4 h-4" />
        </Button>
      </div>
    </div>
  );
}
