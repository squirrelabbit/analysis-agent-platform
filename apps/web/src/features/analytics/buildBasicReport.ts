// analytics mock(DATASETS) → 기본 템플릿 계약(BasicReport) 변환 어댑터.
// 확인 필요: 기초 분석 집계 API가 아직 없어 mock을 템플릿 모양으로 매핑한다.
// API(POST /reports/from_template) 준비 시 이 파일 대신 응답을 그대로 쓰면 된다.
import type {
  BasicBlock,
  BasicReport,
} from "@/features/reports/models/basicTemplate";
import { pct, type DatasetMeta, type ScopeKey } from "./mock";

export function buildBasicReport(
  ds: DatasetMeta,
  scope: ScopeKey,
): BasicReport {
  const cur = ds[scope];

  // §2 분석 개요
  const analysisOverview: BasicBlock = {
    block_id: "b1",
    section_id: "analysis_overview",
    title: "분석 개요",
    layout: [
      {
        panels: [
          {
            view: "stat_grid",
            width: "full",
            data: {
              items: [
                { key: "dataset", label: "데이터셋", value: ds.label, format: "text", sub: ds.ver },
                { key: "period", label: "분석 기간", value: ds.period, format: "text", sub: ds.periodYears },
                { key: "channels", label: "수집 채널", value: ds.channelCount, format: "count", unit: "개 채널" },
              ],
            },
          },
        ],
      },
    ],
  };

  // §3 문서 개요 (전체/최근 진성 문서수·절수)
  const docOverview: BasicBlock = {
    block_id: "b2",
    section_id: "doc_overview",
    title: "문서 개요",
    bare: true,
    layout: [
      {
        panels: [
          {
            view: "stat_cards",
            width: "full",
            data: {
              items: [
                { key: "genuine_all", label: "진성 문서수", value: ds.all.docTotal, format: "count", accent: "muted", badge: "전체 기간", sub: `${ds.all.range} · ${ds.periodYears} 전체` },
                { key: "genuine_recent", label: "진성 문서수", value: ds.recent.docTotal, format: "count", accent: "primary", badge: "최근 연도", sub: ds.recent.range },
                { key: "clause_all", label: "절(clause) 수", value: ds.all.clauseTotal, format: "count", accent: "muted", badge: "전체 기간", sub: `진성 문서에서 추출 · ${ds.periodYears} 전체` },
                { key: "clause_recent", label: "절(clause) 수", value: ds.recent.clauseTotal, format: "count", accent: "primary", badge: "최근 연도", sub: `${ds.recentYear}년 진성 문서 기준` },
              ],
            },
          },
        ],
      },
    ],
  };

  // §4 채널별 진성 문서 분포 (bar + table)
  const channelItems = cur.channels.map((c) => ({
    key: c.key,
    label: c.name,
    count: c.n,
    percent: pct(c.n, cur.docTotal),
  }));
  const channelDist: BasicBlock = {
    block_id: "b3",
    section_id: "channel_distribution",
    title: "채널별 진성 문서 분포",
    unit_basis: "doc",
    layout: [
      {
        panels: [
          { view: "bar", width: "2/3", value_format: "count", data: { total: cur.docTotal, items: channelItems } },
          { view: "table", width: "1/3", data: { total: cur.docTotal, items: channelItems } },
        ],
      },
    ],
  };

  // §5 절 단위 감성 분포 (doughnut + table)
  const sentimentItems = cur.sentiment.map((s) => ({
    key: s.key,
    label: s.name,
    count: s.n,
    percent: pct(s.n, cur.clauseTotal),
  }));
  const sentimentDist: BasicBlock = {
    block_id: "b4",
    section_id: "sentiment_distribution",
    title: "절 단위 감성 분포",
    unit_basis: "clause",
    layout: [
      {
        panels: [
          { view: "doughnut", width: "2/3", value_format: "percent", data: { total: cur.clauseTotal, items: sentimentItems } },
          { view: "table", width: "1/3", data: { total: cur.clauseTotal, items: sentimentItems } },
        ],
      },
    ],
  };

  // §6 유형별 절 분포 (bar + table)
  const aspectItems = cur.types.map((t, i) => ({
    key: `aspect_${i}`,
    label: t.name,
    count: t.n,
    percent: pct(t.n, cur.clauseTotal),
  }));
  const aspectDist: BasicBlock = {
    block_id: "b5",
    section_id: "aspect_distribution",
    title: "유형별 절 분포",
    unit_basis: "clause",
    layout: [
      {
        panels: [
          { view: "bar", width: "2/3", value_format: "count", data: { total: cur.clauseTotal, items: aspectItems } },
          { view: "table", width: "1/3", data: { total: cur.clauseTotal, items: aspectItems } },
        ],
      },
    ],
  };

  // §7 유형별 감성 구성·대비 (100% 누적 + 긍/부정 순위)
  const aspectCount = (
    t: (typeof cur.types)[number],
    key: "pos" | "neu" | "neg",
  ) => Math.round((t.n * t[key]) / 100);
  // 100% 누적은 중립을 빼고 긍정·부정만 100%로 재정규화한다.
  const posPctNorm = (t: (typeof cur.types)[number]) => {
    const base = t.pos + t.neg;
    return base > 0 ? Math.round((t.pos / base) * 100) : 0;
  };
  const aspectSentiment: BasicBlock = {
    block_id: "b6",
    section_id: "aspect_sentiment",
    title: "유형별 감성 구성·대비",
    unit_basis: "clause",
    layout: [
      {
        panels: [
          {
            view: "stacked_bar",
            width: "full",
            value_format: "percent",
            title: "긍정·부정 구성비 (100%, 중립 제외)",
            data: {
              categories: cur.types.map((t, i) => ({ key: `aspect_${i}`, label: t.name, total: t.n })),
              series: [
                { key: "positive", label: "긍정", counts: cur.types.map((t) => aspectCount(t, "pos")), percents: cur.types.map((t) => posPctNorm(t)) },
                { key: "negative", label: "부정", counts: cur.types.map((t) => aspectCount(t, "neg")), percents: cur.types.map((t) => 100 - posPctNorm(t)) },
              ],
            },
          },
        ],
      },
      {
        panels: [
          {
            view: "rank",
            width: "1/2",
            value_format: "count",
            title: "긍정 많은 유형",
            data: {
              items: cur.types
                .map((t) => ({ label: t.name, value: aspectCount(t, "pos") }))
                .sort((a, b) => b.value - a.value)
                .map((it, i) => ({ rank: i + 1, label: it.label, value: it.value })),
            },
          },
          {
            view: "rank",
            width: "1/2",
            value_format: "count",
            title: "부정 많은 유형",
            data: {
              items: cur.types
                .map((t) => ({ label: t.name, value: aspectCount(t, "neg") }))
                .sort((a, b) => b.value - a.value)
                .map((it, i) => ({ rank: i + 1, label: it.label, value: it.value })),
            },
          },
        ],
      },
    ],
  };

  // §8 감성별 상위 키워드 (긍/부정 rank)
  const keywordDist: BasicBlock = {
    block_id: "b7",
    section_id: "keyword_distribution",
    title: "감성별 상위 키워드",
    unit_basis: "clause",
    layout: [
      {
        panels: [
          {
            view: "table",
            width: "1/2",
            title: "긍정 키워드",
            data: {
              columns: ["순위", "키워드", "빈도"],
              rows: cur.kwPos.slice(0, 10).map(([word, n], i) => [i + 1, word, n]),
            },
          },
          {
            view: "table",
            width: "1/2",
            title: "부정 키워드",
            data: {
              columns: ["순위", "키워드", "빈도"],
              rows: cur.kwNeg.slice(0, 10).map(([word, n], i) => [i + 1, word, n]),
            },
          },
        ],
      },
    ],
  };

  return {
    title: "데이터 기초 분석 보고서",
    blocks: [
      analysisOverview,
      docOverview,
      channelDist,
      sentimentDist,
      aspectDist,
      aspectSentiment,
      keywordDist,
    ],
  };
}
