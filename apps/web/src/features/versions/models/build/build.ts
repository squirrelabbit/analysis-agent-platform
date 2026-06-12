import {
  mapCleanBuild,
  type CleanBuild,
  type CleanBuildResponse,
} from "./clean";
import {
  mapGenuinenessBuild,
  type GenuinenessBuild,
  type GenuinenessBuildResponse,
} from "./genuineness";
import {
  mapClauseLabelBuild,
  type ClauseBuild,
  type ClauseBuildResponse,
} from "./clause";
import {
  mapKeywordBuild,
  type KeywordBuild,
  type KeywordBuildResponse,
} from "./keyword";

// 분석별 빌드 응답·모델을 모아 union으로 묶고, build_type으로 분기하는 디스패처를 둔다.
// build.query의 select(mapBuild)가 이 디스패처로 raw 응답을 도메인 모델로 변환한다.

export type BuildResponse =
  | CleanBuildResponse
  | GenuinenessBuildResponse
  | ClauseBuildResponse
  | KeywordBuildResponse;

export type Build =
  | CleanBuild
  | GenuinenessBuild
  | ClauseBuild
  | KeywordBuild;

export const mapBuild = (dto: BuildResponse): Build => {
  switch (dto.build_type) {
    case "clean":
      return mapCleanBuild(dto);
    case "doc_genuineness":
      return mapGenuinenessBuild(dto);
    case "clause_label":
      return mapClauseLabelBuild(dto);
    case "clause_keywords":
      return mapKeywordBuild(dto);
  }
};
