#!/usr/bin/env bash
# scripts/skill_usage_summary.sh
#
# Aggregate `event=skill.usage` log lines into a per-skill usage report.
# Codex 라운드 2A 권고: 8월 이후 skill prune 결정을 1~2주 운영 데이터 기반으로
# 내리기 위해, 매 step 실행마다 control-plane이 emit하는 skill.usage 이벤트를
# 누적 집계한다. count == 0 인 skill은 deadweight 1순위 후보.
#
# Usage:
#   docker compose -f compose.dev.yml logs control-plane 2>&1 \
#     | scripts/skill_usage_summary.sh
#
#   # 또는 파일에서:
#   scripts/skill_usage_summary.sh /path/to/control-plane.log
#
# Output columns:
#   skill_name(view)   집계 키 (issue_summary는 view별 분리)
#   count              총 실행 횟수
#   ok                 status=completed
#   fail               status=failed* (failed_unsupported, failed_ceiling 포함)
#   skip               status=skipped* (skipped_ceiling, skipped_unsupported)
#   avg_ms             평균 duration_ms
#   tokens             total_tokens 누적합 (LLM 의존 skill만 의미 있음)
#   last_seen          가장 최근 실행 timestamp (slog text 포맷의 첫 두 필드)

set -euo pipefail

if [ $# -eq 0 ]; then
    INPUT="/dev/stdin"
else
    INPUT="$1"
fi

awk '
    /event=skill\.usage/ {
        skill = ""; view = ""; status = ""; duration_ms = 0; total_tokens = 0
        for (i = 1; i <= NF; i++) {
            eq = index($i, "=")
            if (eq == 0) continue
            key = substr($i, 1, eq - 1)
            val = substr($i, eq + 1)
            if (key == "skill_name") skill = val
            else if (key == "view") view = val
            else if (key == "status") status = val
            else if (key == "duration_ms") duration_ms = val + 0
            else if (key == "total_tokens") total_tokens = val + 0
        }
        if (skill == "") next
        bucket = skill
        if (view != "") bucket = skill "(" view ")"
        count[bucket]++
        total_duration[bucket] += duration_ms
        total_tokens_sum[bucket] += total_tokens
        if (status == "completed") completed[bucket]++
        else if (status ~ /^failed/) failed[bucket]++
        else if (status ~ /^skipped/) skipped[bucket]++
        last_seen[bucket] = $1 " " $2
    }
    END {
        if (length(count) == 0) {
            print "no skill.usage events found in input"
            exit 0
        }
        printf "%-42s %6s %6s %6s %6s %10s %10s %20s\n", "skill_name(view)", "count", "ok", "fail", "skip", "avg_ms", "tokens", "last_seen"
        printf "%-42s %6s %6s %6s %6s %10s %10s %20s\n", "----------------", "-----", "--", "----", "----", "------", "------", "---------"
        for (k in count) keys[++n] = k
        # sort by count desc (bubble — n is small)
        for (i = 1; i < n; i++) {
            for (j = 1; j < n - i + 1; j++) {
                if (count[keys[j]] < count[keys[j+1]]) {
                    tmp = keys[j]; keys[j] = keys[j+1]; keys[j+1] = tmp
                }
            }
        }
        for (i = 1; i <= n; i++) {
            k = keys[i]
            avg_ms = (count[k] > 0) ? total_duration[k] / count[k] : 0
            printf "%-42s %6d %6d %6d %6d %10.1f %10d %20s\n",
                k, count[k], completed[k]+0, failed[k]+0, skipped[k]+0,
                avg_ms, total_tokens_sum[k]+0, last_seen[k]
        }
    }
' "$INPUT"
