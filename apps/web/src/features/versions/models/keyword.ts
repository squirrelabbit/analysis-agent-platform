// 키워드 분석 탭 모델.
// 키워드 분석은 절 라벨링(clause_label) 결과를 입력으로 하는 파생 분석이다.
// 확인 필요: 백엔드 키워드 집계 엔드포인트는 아직 미정. 현재 hooks/keyword.query.ts가
// 임시 mock을 반환한다. 실제 데이터 소스(백엔드 집계 vs 프론트 집계)가 정해지면
// 이 모델은 그대로 두고 훅의 queryFn만 교체하면 된다.

export type Sentiment = "positive" | "neutral" | "negative";

// 감성 3분류 비율(%) 또는 건수. 합이 100(또는 total)이 되도록 채운다.
export interface SentimentSplit {
  positive: number;
  neutral: number;
  negative: number;
}

// 칭찬/불만 Top N 랭킹 항목. count = 해당 감성 건수.
export interface KeywordRankItem {
  term: string;
  count: number;
}

// Aspect 드릴다운에서 한 aspect에 속한 키워드.
export interface AspectKeyword {
  term: string;
  count: number;
  sentiment: Sentiment; // 키워드의 대표 감성
}

// 절 라벨링 9-aspect 체계의 aspect별 키워드 묶음.
export interface KeywordAspect {
  aspectKey: string; // taxonomy key (예: show_program). 라벨은 aspectLabelOf로 변환.
  sentenceCount: number; // 이 aspect로 분류된 문장 수
  sentiment: SentimentSplit; // 감성 구성(%)
  keywords: AspectKeyword[]; // 빈도순 정렬 가정
  representativeSentence: string;
}

// 상세 테이블 한 행.
export interface KeywordTableItem {
  term: string;
  count: number; // 출현수
  docCount: number; // 등장 문서 수
  sentiment: Sentiment; // 대표 감성
  sentimentPercent: number; // 대표 감성 비율
  aspectKey: string; // 연관 aspect (taxonomy key)
  representativeSentence: string;
}

export interface KeywordSummary {
  totalOccurrences: number; // 총 키워드 출현
  uniqueCount: number; // 고유 키워드 수
  topTerms: string[]; // 최다 출현 키워드(동률 가능)
  docCoveragePercent: number; // 키워드가 1개 이상 등장한 문서 비율
}

export interface KeywordResult {
  // clause_label 빌드 상태에 의존. 미완료면 summary 없음.
  status: string;
  summary?: KeywordSummary;
  positiveTop: KeywordRankItem[]; // 칭찬 키워드 Top N
  negativeTop: KeywordRankItem[]; // 불만 키워드 Top N
  aspects: KeywordAspect[]; // 문장수 내림차순 가정
  items: KeywordTableItem[]; // 전체 키워드(출현수 내림차순). 표는 클라이언트 페이징/필터.
}