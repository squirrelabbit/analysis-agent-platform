export interface Pagination {
  limit: number;
  offset: number;
  total: number;
}

export interface FormProps<T> {
  formId: string;
  onSubmit: (data: T) => Promise<void>;
  onSuccess: () => void;
}