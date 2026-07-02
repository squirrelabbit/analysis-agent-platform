package service

import "testing"

// 진성 결과 화면 원문 URL 버튼 — clean source_json에서 원문 URL 추출 잠금.
func TestExtractSourceURL(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "전용 URL 컬럼(값 전체가 url)",
			in:   `{"제목":"강릉 야행","본문":"좋았어요","URL":"https://cafe.naver.com/no1foodtruck/13"}`,
			want: "https://cafe.naver.com/no1foodtruck/13",
		},
		{
			name: "본문에 url 포함돼 있어도 본문은 제외(공백 포함)",
			in:   `{"본문":"여기 보세요 https://x.com/a 좋아요","링크":"http://blog.example.com/p/1"}`,
			want: "http://blog.example.com/p/1",
		},
		{
			name: "URL 없음",
			in:   `{"제목":"강릉","본문":"후기입니다"}`,
			want: "",
		},
		{
			name: "빈 문자열",
			in:   "",
			want: "",
		},
		{
			name: "JSON 아님",
			in:   "not json",
			want: "",
		},
		{
			name: "키 정렬로 결정적(여러 url 중 키 사전순 첫번째)",
			in:   `{"b_url":"https://b.com","a_url":"https://a.com"}`,
			want: "https://a.com",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractSourceURL(tc.in); got != tc.want {
				t.Fatalf("extractSourceURL = %q, want %q", got, tc.want)
			}
		})
	}
}
