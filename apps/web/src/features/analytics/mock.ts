// 데이터 기초 분석 보고서 — mock 데이터.
// 확인 필요: 기초 분석 집계 API가 아직 없어 디자인 시안(데이터 기초 분석 보고서.html)의
// DATASETS를 그대로 포팅했다. API 준비 시 이 모듈만 실데이터 fetch로 교체.

export type ScopeKey = "recent" | "all";
export type DatasetKey = "festival_sns" | "insta_data";

export interface ChannelDatum {
  // 채널 아이콘 매핑 키 (insta/blog/news/comm/youtube/post/reels/comment).
  key: string;
  name: string;
  n: number;
}

export interface SentimentDatum {
  key: "pos" | "neu" | "neg";
  name: string;
  n: number;
  color: string; // hex
}

// pos/neu/neg는 해당 유형 절 중 각 감성의 비중(%) — 합 100.
export interface TypeDatum {
  name: string;
  n: number;
  pos: number;
  neu: number;
  neg: number;
}

export type KeywordRow = [word: string, count: number];

export interface ScopeData {
  range: string;
  label: string;
  docTotal: number;
  clauseTotal: number;
  channels: ChannelDatum[];
  sentiment: SentimentDatum[];
  types: TypeDatum[];
  kwPos: KeywordRow[];
  kwNeg: KeywordRow[];
}

export interface DatasetMeta {
  label: string;
  ver: string;
  period: string;
  periodYears: string;
  channelCount: number;
  recentYear: string;
  allShort: string;
  recent: ScopeData;
  all: ScopeData;
}

// 감성 색 — 코드베이스 공용 상수(constants/sentiment.ts)와 동일 hex.
const POS = "#10b981"; // emerald-500
const NEU = "#a1a1aa"; // zinc-400
const NEG = "#ef4444"; // red-500

export const DATASETS: Record<DatasetKey, DatasetMeta> = {
  festival_sns: {
    label: "festival_sns",
    ver: "v3",
    period: "2023.01 ~ 2025.12",
    periodYears: "3년",
    channelCount: 5,
    recentYear: "2025",
    allShort: "2023–2025",
    recent: {
      range: "2025.01.01 ~ 2025.12.31",
      label: "최근 연도",
      docTotal: 395,
      clauseTotal: 3072,
      channels: [
        { key: "insta", name: "인스타그램", n: 142 },
        { key: "blog", name: "블로그", n: 98 },
        { key: "news", name: "뉴스", n: 71 },
        { key: "comm", name: "커뮤니티", n: 54 },
        { key: "youtube", name: "유튜브", n: 30 },
      ],
      sentiment: [
        { key: "pos", name: "긍정", n: 1788, color: POS },
        { key: "neu", name: "중립", n: 528, color: NEU },
        { key: "neg", name: "부정", n: 756, color: NEG },
      ],
      types: [
        { name: "공연/프로그램", n: 742, pos: 78, neu: 12, neg: 10 },
        { name: "분위기/경관", n: 588, pos: 81, neu: 11, neg: 8 },
        { name: "운영/서비스", n: 514, pos: 34, neu: 18, neg: 48 },
        { name: "편의시설/혼잡도", n: 446, pos: 22, neu: 15, neg: 63 },
        { name: "음식/먹거리", n: 412, pos: 49, neu: 20, neg: 31 },
        { name: "교통/접근성", n: 370, pos: 18, neu: 14, neg: 68 },
      ],
      kwPos: [
        ["공연", 312],
        ["분위기", 287],
        ["야경", 241],
        ["한복", 198],
        ["조명", 176],
        ["사진", 154],
        ["포토존", 139],
        ["가족", 121],
        ["추억", 108],
        ["재방문", 94],
      ],
      kwNeg: [
        ["대기시간", 218],
        ["주차", 193],
        ["혼잡", 171],
        ["화장실", 142],
        ["셔틀버스", 128],
        ["가격", 116],
        ["안내부족", 97],
        ["소음", 83],
        ["대기열", 71],
        ["불친절", 58],
      ],
    },
    all: {
      range: "2023.01.01 ~ 2025.12.31",
      label: "전체 기간",
      docTotal: 1182,
      clauseTotal: 8940,
      channels: [
        { key: "insta", name: "인스타그램", n: 408 },
        { key: "blog", name: "블로그", n: 291 },
        { key: "news", name: "뉴스", n: 214 },
        { key: "comm", name: "커뮤니티", n: 169 },
        { key: "youtube", name: "유튜브", n: 100 },
      ],
      sentiment: [
        { key: "pos", name: "긍정", n: 5131, color: POS },
        { key: "neu", name: "중립", n: 1592, color: NEU },
        { key: "neg", name: "부정", n: 2217, color: NEG },
      ],
      types: [
        { name: "공연/프로그램", n: 2166, pos: 76, neu: 13, neg: 11 },
        { name: "분위기/경관", n: 1690, pos: 80, neu: 12, neg: 8 },
        { name: "운영/서비스", n: 1502, pos: 36, neu: 17, neg: 47 },
        { name: "편의시설/혼잡도", n: 1296, pos: 24, neu: 15, neg: 61 },
        { name: "음식/먹거리", n: 1198, pos: 47, neu: 21, neg: 32 },
        { name: "교통/접근성", n: 1088, pos: 20, neu: 13, neg: 67 },
      ],
      kwPos: [
        ["공연", 902],
        ["분위기", 831],
        ["야경", 712],
        ["한복", 588],
        ["조명", 521],
        ["사진", 463],
        ["포토존", 402],
        ["가족", 357],
        ["추억", 318],
        ["재방문", 276],
      ],
      kwNeg: [
        ["대기시간", 642],
        ["주차", 571],
        ["혼잡", 498],
        ["화장실", 417],
        ["셔틀버스", 368],
        ["가격", 329],
        ["안내부족", 284],
        ["소음", 241],
        ["대기열", 203],
        ["불친절", 167],
      ],
    },
  },
  insta_data: {
    label: "insta_data",
    ver: "v1",
    period: "2024.06 ~ 2025.12",
    periodYears: "약 1.5년",
    channelCount: 3,
    recentYear: "2025",
    allShort: "2024–2025",
    recent: {
      range: "2025.01.01 ~ 2025.12.31",
      label: "최근 연도",
      docTotal: 268,
      clauseTotal: 1840,
      channels: [
        { key: "post", name: "게시글", n: 138 },
        { key: "reels", name: "릴스", n: 79 },
        { key: "comment", name: "댓글", n: 51 },
      ],
      sentiment: [
        { key: "pos", name: "긍정", n: 1158, color: POS },
        { key: "neu", name: "중립", n: 322, color: NEU },
        { key: "neg", name: "부정", n: 360, color: NEG },
      ],
      types: [
        { name: "공연/프로그램", n: 470, pos: 82, neu: 11, neg: 7 },
        { name: "분위기/경관", n: 432, pos: 85, neu: 10, neg: 5 },
        { name: "음식/먹거리", n: 320, pos: 55, neu: 22, neg: 23 },
        { name: "운영/서비스", n: 250, pos: 40, neu: 19, neg: 41 },
        { name: "편의시설/혼잡도", n: 208, pos: 28, neu: 16, neg: 56 },
        { name: "교통/접근성", n: 160, pos: 24, neu: 15, neg: 61 },
      ],
      kwPos: [
        ["분위기", 214],
        ["공연", 198],
        ["예쁜", 176],
        ["사진", 159],
        ["릴스", 141],
        ["조명", 128],
        ["감성", 112],
        ["데이트", 98],
        ["소통", 84],
        ["재방문", 73],
      ],
      kwNeg: [
        ["혼잡", 168],
        ["대기", 139],
        ["가격", 121],
        ["주차", 104],
        ["품절", 88],
        ["소음", 76],
        ["배송", 64],
        ["화장실", 55],
        ["안내", 47],
        ["광고", 39],
      ],
    },
    all: {
      range: "2024.06.01 ~ 2025.12.31",
      label: "전체 기간",
      docTotal: 612,
      clauseTotal: 4210,
      channels: [
        { key: "post", name: "게시글", n: 312 },
        { key: "reels", name: "릴스", n: 184 },
        { key: "comment", name: "댓글", n: 116 },
      ],
      sentiment: [
        { key: "pos", name: "긍정", n: 2610, color: POS },
        { key: "neu", name: "중립", n: 760, color: NEU },
        { key: "neg", name: "부정", n: 840, color: NEG },
      ],
      types: [
        { name: "공연/프로그램", n: 1070, pos: 80, neu: 12, neg: 8 },
        { name: "분위기/경관", n: 988, pos: 84, neu: 11, neg: 5 },
        { name: "음식/먹거리", n: 740, pos: 54, neu: 22, neg: 24 },
        { name: "운영/서비스", n: 572, pos: 41, neu: 18, neg: 41 },
        { name: "편의시설/혼잡도", n: 480, pos: 29, neu: 16, neg: 55 },
        { name: "교통/접근성", n: 360, pos: 25, neu: 14, neg: 61 },
      ],
      kwPos: [
        ["분위기", 498],
        ["공연", 456],
        ["예쁜", 408],
        ["사진", 367],
        ["릴스", 321],
        ["조명", 289],
        ["감성", 254],
        ["데이트", 221],
        ["소통", 188],
        ["재방문", 162],
      ],
      kwNeg: [
        ["혼잡", 384],
        ["대기", 321],
        ["가격", 278],
        ["주차", 241],
        ["품절", 203],
        ["소음", 172],
        ["배송", 146],
        ["화장실", 121],
        ["안내", 98],
        ["광고", 79],
      ],
    },
  },
};

// ── 표시 helper ──────────────────────────────────────────────
export const fmt = (n: number): string => n.toLocaleString();
// 소수 첫째자리 퍼센트.
export const pct = (n: number, total: number): number =>
  total > 0 ? Math.round((n / total) * 1000) / 10 : 0;
