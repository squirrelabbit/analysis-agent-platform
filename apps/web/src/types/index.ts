export type Project = {
  project_id: string;
  name: string;
  description: string;
  dataset_version_count: number;
  scenario_count: number;
  prompt_count: number;
};

export type Scenario = {
  scenario_id: string;
  project_id: string;
  planning_mode: string;
  user_query: string;
  query_type: string;
  interpretation: string;
  analysis_scope: string;
};

export type Dataset = {
  dataset_id: string;
  project_id: string;
  name: string;
  description: string | null;
  data_type: string;
};

export type Operation = "prepare" | "sentiment"

export type Prompt = {
  project_id: string
  version: string
  operation: Operation
  title: string
  status: string
  summary: string
  content: string
  content_hash: string
}
