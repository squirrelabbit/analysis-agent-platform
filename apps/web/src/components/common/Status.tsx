import { AlertCircle, CheckCircle, Loader } from "lucide-react";

export const getStatusColor = (status: string) => {
  switch (status) {
    case "ready":
      return "bg-emerald-100 text-emerald-800";
    case "running":
      return "bg-yellow-100 text-yellow-800";
    case "not_requested":
      return "bg-slate-100 text-slate-800";
    case "failed":
      return "bg-red-100 text-red-800";
    case "completed":
      return "bg-emerald-100 text-emerald-800";
    default:
      return "bg-slate-100 text-slate-800";
  }
};

export const getStatusLabel = (status: string) => {
  switch (status) {
    case "ready":
      return "완료";
    case "running":
      return "실행중";
    case "not_requested":
      return "대기중";
    case "failed":
      return "실패";
    case "completed":
      return "완료";
    default:
      return status;
  }
};

export const getStatusIcon = (status: string) => {
  switch (status) {
    case "ready":
      return <CheckCircle className="w-5 h-5 text-emerald-600" />;
    case "running":
      return <Loader className="w-5 h-5 text-yellow-600 animate-spin" />;
    case "not_requested":
      return <AlertCircle className="w-5 h-5 text-slate-600" />;
    case "failed":
      return <AlertCircle className="w-5 h-5 text-red-600" />;
    case "completed":
      return <CheckCircle className="w-5 h-5 text-emerald-600" />;
    default:
      return null;
  }
};