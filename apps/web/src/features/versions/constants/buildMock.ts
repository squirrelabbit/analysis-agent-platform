export const cleanResult = {
    "build_type": "clean",
    "status": "completed",
    "job_id": "22c7ba9a-9a0c-4d1b-adc0-ea2c136f5047",
    "started_at": "2026-05-27T17:12:24.734926+09:00",
    "completed_at": "2026-05-27T17:12:25.967654+09:00",
    "duration_seconds": 1.232728,
    "error_message": null,
    "progress": {
        "percent": 100,
        "processed_rows": 2121,
        "total_rows": 2121,
        "message": "clean completed",
        "updated_at": "2026-05-27T17:12:25.956132+09:00"
    },
    "summary": {
        "clean_reduced_char_count": 280,
        "cleaned_input_char_count": 65286,
        "dropped_count": 0,
        "input_row_count": 2121,
        "kept_count": 2121,
        "output_row_count": 2121,
        "source_input_char_count": 65566,
        "text_column": "제목",
        "text_columns": [
            "제목"
        ]
    }
}

export const genuinenessResult = {
    "build_type": "doc_genuineness",
    "status": "running",
    "job_id": "587a1f53-b176-442c-9db3-8c5fd2b90809",
    "started_at": "2026-05-27T17:16:31.80578+09:00",
    "completed_at": null,
    "duration_seconds": 512.84292972,
    "error_message": null,
    "progress": {
        "percent": 18.39,
        "processed_rows": 390,
        "total_rows": 2121,
        "eta_seconds": 1287.45,
        "message": "doc_genuineness processing",
        "updated_at": "2026-05-27T17:25:02.508876+09:00"
    },
    "applied": {
        "prompt_version": "v1"
    },
    "summary": {
        "genuineness": {
            "genuine_review": 9,
            "non_review": 296,
            "uncertain": 71
        },
        "total": 376
    },
    "items": [
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:0",
            "genuineness": "non_review",
            "reason": "푸드트럭 모집 공고, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:1",
            "genuineness": "non_review",
            "reason": "축제 리스트 소개, 강릉 국가유산야행 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:10",
            "genuineness": "non_review",
            "reason": "행사 일정 안내, 개인 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:100",
            "genuineness": "non_review",
            "reason": "개막 안내 기사, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:101",
            "genuineness": "non_review",
            "reason": "사업 선정 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:102",
            "genuineness": "non_review",
            "reason": "행사 기대·예고 문구, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:103",
            "genuineness": "non_review",
            "reason": "모집 공고이며 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:104",
            "genuineness": "non_review",
            "reason": "행사 소개만 있고 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:105",
            "genuineness": "uncertain",
            "reason": "해시태그·뱃지만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:106",
            "genuineness": "non_review",
            "reason": "행사 소개만 있고 방문 언급이 없어 비진성글",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:107",
            "genuineness": "non_review",
            "reason": "서포터즈 모집 공고, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:108",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 다른 행사 소개",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:109",
            "genuineness": "uncertain",
            "reason": "해시태그·제목만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:11",
            "genuineness": "non_review",
            "reason": "방문 후기 없이 정보 질문만 있음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:110",
            "genuineness": "non_review",
            "reason": "행사 소개만 있고 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:111",
            "genuineness": "uncertain",
            "reason": "해시태그·제목만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:112",
            "genuineness": "non_review",
            "reason": "행사 소개만 있고 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:113",
            "genuineness": "uncertain",
            "reason": "해시태그·짧은 문구만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:114",
            "genuineness": "non_review",
            "reason": "온라인 서포터즈 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:115",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 행사 인증서 수여 소식",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:116",
            "genuineness": "non_review",
            "reason": "행사 홍보 문구, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:117",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 전혀 다른 내용",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:118",
            "genuineness": "non_review",
            "reason": "공고 내용으로 강릉 국가유산야행 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:119",
            "genuineness": "non_review",
            "reason": "축제 추천 리스트형 소개, 본인 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:12",
            "genuineness": "non_review",
            "reason": "축제 총정리 글, 강릉 국가유산야행 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:120",
            "genuineness": "non_review",
            "reason": "행사 개최 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:121",
            "genuineness": "non_review",
            "reason": "푸드트럭 모집 공고, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:122",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:123",
            "genuineness": "uncertain",
            "reason": "해시태그·내용 부족, 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:124",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 행사 관련 글 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:125",
            "genuineness": "non_review",
            "reason": "영상공모전 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:126",
            "genuineness": "non_review",
            "reason": "서포터즈 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:127",
            "genuineness": "uncertain",
            "reason": "해시태그·내용 부족, 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:128",
            "genuineness": "non_review",
            "reason": "행사 선정 소식 안내, 개인 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:129",
            "genuineness": "non_review",
            "reason": "홍보단 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:13",
            "genuineness": "non_review",
            "reason": "모델 모집 공고, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:130",
            "genuineness": "non_review",
            "reason": "축제 일정표 소개, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:131",
            "genuineness": "non_review",
            "reason": "축제 일정 소개, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:132",
            "genuineness": "uncertain",
            "reason": "해시태그·내용 부족, 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:133",
            "genuineness": "uncertain",
            "reason": "해시태그·내용 부족, 강릉국가유산야행 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:134",
            "genuineness": "non_review",
            "reason": "공연예술가 모집 공고, 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:135",
            "genuineness": "uncertain",
            "reason": "해시태그·제목만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:136",
            "genuineness": "non_review",
            "reason": "공연 출연 안내, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:137",
            "genuineness": "non_review",
            "reason": "행사 소개만 있고 개인 방문 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:138",
            "genuineness": "non_review",
            "reason": "일정 안내만 있어 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:139",
            "genuineness": "uncertain",
            "reason": "해시태그·짧은 문구만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:14",
            "genuineness": "non_review",
            "reason": "단순 정보·참고 문구만 있어 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:140",
            "genuineness": "non_review",
            "reason": "우표전시회 안내, 개인 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:141",
            "genuineness": "non_review",
            "reason": "기사 스크랩 소개, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:142",
            "genuineness": "non_review",
            "reason": "관아해설사 양성교육 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:143",
            "genuineness": "non_review",
            "reason": "관아해설사 수강생 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:144",
            "genuineness": "uncertain",
            "reason": "해시태그·짧은 문구만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:145",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 일반 축제 소개",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:146",
            "genuineness": "non_review",
            "reason": "셀러 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:147",
            "genuineness": "non_review",
            "reason": "축제 일정 소개만 있어 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:148",
            "genuineness": "non_review",
            "reason": "추천 글이며 방문 후기 내용 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:149",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없이 다른 여행 소개",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:15",
            "genuineness": "non_review",
            "reason": "서포터즈 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:150",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 일반 축제 언급",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:151",
            "genuineness": "non_review",
            "reason": "관광지 소개형 제목, 방문 후기 내용 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:152",
            "genuineness": "uncertain",
            "reason": "내용이 부족해 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:153",
            "genuineness": "non_review",
            "reason": "해설사와 역사투어 참가자 모집 공지",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:154",
            "genuineness": "non_review",
            "reason": "행사와 무관한 레저팀 내용, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:155",
            "genuineness": "non_review",
            "reason": "축제 일정 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:156",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 다른 여행 홍보 글",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:157",
            "genuineness": "non_review",
            "reason": "축제 일정 소개, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:158",
            "genuineness": "non_review",
            "reason": "축제 일정 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:159",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없이 다른 여행 소개",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:16",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없이 드론 라이트쇼 영상",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:160",
            "genuineness": "non_review",
            "reason": "프리버스킹 참가자 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:161",
            "genuineness": "non_review",
            "reason": "서포터즈 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:162",
            "genuineness": "non_review",
            "reason": "행사 소개 문구만 있어 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:163",
            "genuineness": "non_review",
            "reason": "홍보단 모집 및 혜택 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:164",
            "genuineness": "non_review",
            "reason": "행사 언급 없이 인터넷 가입 내용, 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:165",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 일반 명소 현황",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:166",
            "genuineness": "non_review",
            "reason": "푸드트럭 모집 공고, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:167",
            "genuineness": "non_review",
            "reason": "버스킹 공모 안내, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:168",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 일반 관광 이벤트 소개",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:169",
            "genuineness": "non_review",
            "reason": "축제 일정표 소개, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:17",
            "genuineness": "non_review",
            "reason": "축제 리스트 소개, 개인 방문 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:170",
            "genuineness": "non_review",
            "reason": "참가자 모집 공고, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:171",
            "genuineness": "non_review",
            "reason": "행사 일정·퍼레이드 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:172",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없음, 일반 관광 소개",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:173",
            "genuineness": "non_review",
            "reason": "행사 일정·장소 소개 등 홍보성 내용",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:174",
            "genuineness": "uncertain",
            "reason": "해시태그·제목만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:175",
            "genuineness": "uncertain",
            "reason": "해시태그·짧은 문구만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:176",
            "genuineness": "uncertain",
            "reason": "해시태그·짧은 문구만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:177",
            "genuineness": "non_review",
            "reason": "방문자 수 기록 등 뉴스형 정보, 개인 방문 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:178",
            "genuineness": "non_review",
            "reason": "전시회 안내·홍보 글, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:179",
            "genuineness": "uncertain",
            "reason": "해시태그·내용 부족, 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:18",
            "genuineness": "non_review",
            "reason": "푸드트럭 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:180",
            "genuineness": "non_review",
            "reason": "축제 정보 제공, 강릉 국가유산야행 언급 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:181",
            "genuineness": "non_review",
            "reason": "우수사업 선정 안내, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:182",
            "genuineness": "uncertain",
            "reason": "해시태그·짧은 문구만 있어 방문 여부 판단 불가",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:183",
            "genuineness": "non_review",
            "reason": "사업 소개 문구, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:184",
            "genuineness": "non_review",
            "reason": "푸드트럭 모집 공지, 방문 후기 아님",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:185",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없이 다른 행사 소개",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:186",
            "genuineness": "non_review",
            "reason": "강릉 국가유산야행 언급 없이 일반 여행 추천",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:187",
            "genuineness": "non_review",
            "reason": "축제 일정표 소개, 방문 후기 없음",
            "source": "lloa"
        },
        {
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:188",
            "genuineness": "non_review",
            "reason": "주제와 무관한 내용, 강릉 국가유산야행 언급 없음",
            "source": "lloa"
        }
    ],
    "pagination": {
        "limit": 100,
        "offset": 0,
        "total": 376
    }
}
export const clauseLabelResult = {
    "build_type": "clause_label",
    "status": "completed",
    "job_id": "d6b7899f-943b-4644-936e-b3bd03531df5",
    "started_at": "2026-05-27T17:28:53.987318+09:00",
    "completed_at": "2026-05-27T17:28:55.200099+09:00",
    "duration_seconds": 1.212781,
    "error_message": null,
    "progress": {
        "percent": 100,
        "processed_rows": 2121,
        "total_rows": 2121,
        "message": "clause_label completed",
        "updated_at": "2026-05-27T17:28:55.189783+09:00"
    },
    "applied": {
        "prompt_version": "v3"
    },
    "summary": {
        "aspect": {
            "etc": 3
        },
        "sentiment": {
            "neutral": 3
        },
        "total": 3
    },
    "items": [
        {
            "aspect": "etc",
            "clause": "주말에 강릉문화유산야행다녀왔어요",
            "clause_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:64-0",
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:64",
            "sentiment": "neutral",
            "source": "lloa"
        },
        {
            "aspect": "etc",
            "clause": "강릉문화유산야행다녀왔어요",
            "clause_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:82-0",
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:82",
            "sentiment": "neutral",
            "source": "lloa"
        },
        {
            "aspect": "etc",
            "clause": "강릉야행 다녀왔어요",
            "clause_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:194-0",
            "doc_id": "8e63502f-2225-487d-baa1-c3d0ef545be5:row:194",
            "sentiment": "neutral",
            "source": "lloa"
        }
    ],
    "pagination": {
        "limit": 100,
        "offset": 0,
        "total": 3
    }
}

export const pipelineResult = {
    "dataset_version_id": "8e63502f-2225-487d-baa1-c3d0ef545be5",
    "created_at": "2026-05-27T16:51:51.427177+09:00",
    "is_active": true,
    "row_count": 2121,
    "column_count": 10,
    "columns": [
        "수집ID(고유)",
        "게시일",
        "수집채널",
        "수집키워드",
        "제목",
        "본문",
        "작성자",
        "URL",
        "댓글수",
        "좋아요 수"
    ],
    "byte_size": 17319276,
    "clean": {
        "status": "ready",
        "completed_at": "2026-05-27T17:12:25.957977+09:00",
        "summary": {
            "input_row_count": 2121,
            "output_row_count": 2121,
            "kept_count": 2121,
            "dropped_count": 0,
            "text_column": "제목",
            "text_columns": [
                "제목"
            ],
            "text_joiner": "\n\n",
            "source_input_char_count": 65566,
            "cleaned_input_char_count": 65286,
            "clean_reduced_char_count": 280
        }
    },
    "doc_genuineness": {
        "status": "running"
    },
    "clause_label": {
        "status": ""
    }
}