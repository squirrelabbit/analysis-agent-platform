import type { KeywordResult } from "../models/keyword";

// ─────────────────────────────────────────────────────────────────────────────
// 확인 필요: 키워드 분석 백엔드 엔드포인트는 아직 미정이다.
// 이 훅은 현재 festival 예시 기반 임시 mock을 동기 반환한다.
// 데이터 소스(백엔드 집계 vs clause_label 프론트 집계)가 결정되면
// 이 파일의 MOCK / useKeyword 내부만 react-query 호출로 교체하면 되고,
// KeywordTab 컴포넌트와 models/keyword.ts는 그대로 둔다.
// ─────────────────────────────────────────────────────────────────────────────

// aspectKey는 config/taxonomies/festival-v2.json의 9-aspect key와 일치시킨다.
const MOCK: KeywordResult = {
  status: "completed",
  summary: {
    totalOccurrences: 1883,
    uniqueCount: 142,
    topTerms: ["야경", "한복"],
    docCoveragePercent: 86,
  },
  // 칭찬/불만 = 긍정/부정 건수 기준 Top 5.
  positiveTop: [
    { term: "한복", count: 146 },
    { term: "야경", count: 145 },
    { term: "먹거리", count: 79 },
    { term: "포토존", count: 50 },
    { term: "조명", count: 49 },
  ],
  negativeTop: [
    { term: "대기시간", count: 93 },
    { term: "주차", count: 78 },
    { term: "혼잡", count: 46 },
    { term: "화장실", count: 33 },
    { term: "셔틀버스", count: 20 },
  ],
  // 문장수 내림차순.
  aspects: [
    {
      aspectKey: "ambiance_scenery",
      sentenceCount: 88,
      sentiment: { positive: 80, neutral: 13, negative: 7 },
      keywords: [
        { term: "야경", count: 214, sentiment: "positive" },
        { term: "한복", count: 197, sentiment: "positive" },
        { term: "포토존", count: 92, sentiment: "positive" },
        { term: "조명", count: 75, sentiment: "positive" },
        { term: "분위기", count: 30, sentiment: "positive" },
      ],
      representativeSentence: "밤에 보는 한옥 야경이 정말 환상적이었어요",
    },
    {
      aspectKey: "show_program",
      sentenceCount: 46,
      sentiment: { positive: 74, neutral: 17, negative: 9 },
      keywords: [
        { term: "프로그램", count: 80, sentiment: "neutral" },
        { term: "공연", count: 38, sentiment: "positive" },
        { term: "드론쇼", count: 29, sentiment: "positive" },
        { term: "버스킹", count: 18, sentiment: "positive" },
      ],
      representativeSentence: "드론쇼가 진짜 너무 환상적이었고 10분 넘게 몰입해서 봤어요",
    },
    {
      aspectKey: "operation_service",
      sentenceCount: 42,
      sentiment: { positive: 19, neutral: 24, negative: 57 },
      keywords: [
        { term: "대기시간", count: 152, sentiment: "negative" },
        { term: "안내", count: 52, sentiment: "neutral" },
        { term: "스태프", count: 22, sentiment: "positive" },
      ],
      representativeSentence: "체험 부스 대기시간이 너무 길어서 결국 못 하고 돌아왔어요",
    },
    {
      aspectKey: "food",
      sentenceCount: 24,
      sentiment: { positive: 62, neutral: 25, negative: 13 },
      keywords: [
        { term: "먹거리", count: 138, sentiment: "positive" },
        { term: "야시장", count: 34, sentiment: "positive" },
        { term: "푸드트럭", count: 21, sentiment: "positive" },
      ],
      representativeSentence: "야시장 먹거리가 다양하고 가성비도 좋았어요",
    },
    {
      aspectKey: "experience_booth",
      sentenceCount: 22,
      sentiment: { positive: 64, neutral: 23, negative: 13 },
      keywords: [
        { term: "체험부스", count: 41, sentiment: "positive" },
        { term: "스탬프", count: 18, sentiment: "positive" },
        { term: "만들기", count: 12, sentiment: "neutral" },
      ],
      representativeSentence: "체험 부스도 다양하고 스탬프 다 모았어요",
    },
    {
      aspectKey: "facility_crowd",
      sentenceCount: 20,
      sentiment: { positive: 15, neutral: 25, negative: 60 },
      keywords: [
        { term: "혼잡", count: 70, sentiment: "negative" },
        { term: "화장실", count: 60, sentiment: "negative" },
        { term: "인파", count: 33, sentiment: "negative" },
        { term: "쉼터", count: 15, sentiment: "neutral" },
      ],
      representativeSentence: "화장실이 너무 부족했어요",
    },
    {
      aspectKey: "access_traffic",
      sentenceCount: 18,
      sentiment: { positive: 11, neutral: 22, negative: 67 },
      keywords: [
        { term: "주차", count: 111, sentiment: "negative" },
        { term: "셔틀버스", count: 40, sentiment: "negative" },
        { term: "교통", count: 28, sentiment: "negative" },
      ],
      representativeSentence: "주차 공간이 부족해서 한참 헤맸습니다",
    },
    {
      aspectKey: "price_cost",
      sentenceCount: 8,
      sentiment: { positive: 50, neutral: 38, negative: 12 },
      keywords: [
        { term: "입장료", count: 48, sentiment: "neutral" },
        { term: "가성비", count: 22, sentiment: "positive" },
        { term: "달고나", count: 9, sentiment: "negative" },
      ],
      representativeSentence: "입장료가 무료라 가성비가 뛰어났어요",
    },
    {
      aspectKey: "etc",
      sentenceCount: 6,
      sentiment: { positive: 17, neutral: 66, negative: 17 },
      keywords: [
        { term: "내년", count: 17, sentiment: "positive" },
        { term: "추천", count: 14, sentiment: "positive" },
      ],
      representativeSentence: "내년에도 또 오고 싶어요",
    },
  ],
  // 출현수 내림차순. 표는 클라이언트에서 검색/감성 필터/페이징.
  items: [
    { term: "야경", count: 214, docCount: 41, sentiment: "positive", sentimentPercent: 68, aspectKey: "ambiance_scenery", representativeSentence: "밤에 보는 한옥 야경이 정말 환상적이었어요" },
    { term: "한복", count: 197, docCount: 38, sentiment: "positive", sentimentPercent: 74, aspectKey: "ambiance_scenery", representativeSentence: "한복 입고 사진 찍기 좋은 포토존이 많았습니다" },
    { term: "대기시간", count: 152, docCount: 33, sentiment: "negative", sentimentPercent: 61, aspectKey: "operation_service", representativeSentence: "체험 부스 대기시간이 너무 길어서 아쉬웠어요" },
    { term: "먹거리", count: 138, docCount: 29, sentiment: "positive", sentimentPercent: 57, aspectKey: "food", representativeSentence: "야시장 먹거리가 다양하고 가성비도 좋았어요" },
    { term: "주차", count: 111, docCount: 27, sentiment: "negative", sentimentPercent: 70, aspectKey: "access_traffic", representativeSentence: "주차 공간이 부족해서 한참 헤맸습니다" },
    { term: "포토존", count: 92, docCount: 24, sentiment: "positive", sentimentPercent: 54, aspectKey: "ambiance_scenery", representativeSentence: "포토존이 곳곳에 있어 사진 찍기 좋았어요" },
    { term: "프로그램", count: 80, docCount: 22, sentiment: "neutral", sentimentPercent: 45, aspectKey: "show_program", representativeSentence: "프로그램 구성이 무난했어요" },
    { term: "조명", count: 75, docCount: 21, sentiment: "positive", sentimentPercent: 65, aspectKey: "ambiance_scenery", representativeSentence: "조명이 켜지니까 분위기가 너무 예뻤어요" },
    { term: "혼잡", count: 70, docCount: 20, sentiment: "negative", sentimentPercent: 65, aspectKey: "facility_crowd", representativeSentence: "사람이 너무 많아서 혼잡했어요" },
    { term: "화장실", count: 60, docCount: 18, sentiment: "negative", sentimentPercent: 55, aspectKey: "facility_crowd", representativeSentence: "화장실이 너무 부족했어요" },
    { term: "안내", count: 52, docCount: 17, sentiment: "neutral", sentimentPercent: 48, aspectKey: "operation_service", representativeSentence: "안내가 조금 부족했지만 직원분들은 친절했어요" },
    { term: "입장료", count: 48, docCount: 16, sentiment: "neutral", sentimentPercent: 50, aspectKey: "price_cost", representativeSentence: "입장료가 무료라 가성비가 뛰어났어요" },
    { term: "셔틀버스", count: 40, docCount: 14, sentiment: "negative", sentimentPercent: 50, aspectKey: "access_traffic", representativeSentence: "셔틀버스 배차 간격이 너무 길었어요" },
    { term: "공연", count: 38, docCount: 13, sentiment: "positive", sentimentPercent: 71, aspectKey: "show_program", representativeSentence: "버스킹 공연이 분위기를 살렸어요" },
    { term: "야시장", count: 34, docCount: 12, sentiment: "positive", sentimentPercent: 62, aspectKey: "food", representativeSentence: "야시장 분위기가 정말 좋았어요" },
    { term: "인파", count: 33, docCount: 11, sentiment: "negative", sentimentPercent: 58, aspectKey: "facility_crowd", representativeSentence: "인파가 몰려서 이동이 힘들었어요" },
    { term: "분위기", count: 30, docCount: 11, sentiment: "positive", sentimentPercent: 80, aspectKey: "ambiance_scenery", representativeSentence: "전체적으로 분위기가 따뜻했어요" },
    { term: "교통", count: 28, docCount: 10, sentiment: "negative", sentimentPercent: 60, aspectKey: "access_traffic", representativeSentence: "교통이 불편해서 접근이 어려웠어요" },
  ],
};

/**
 * 키워드 분석 결과 조회.
 * 현재는 mock 동기 반환(로딩/에러 없음). 백엔드 연동 시 react-query로 교체.
 */
export const useKeyword = (): { data: KeywordResult; isLoading: boolean } => {
  return { data: MOCK, isLoading: false };
};