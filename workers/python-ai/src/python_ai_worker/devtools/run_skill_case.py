from __future__ import annotations

import argparse
import json
import sys

from .skill_cases import available_skill_cases, run_skill_case, validate_skill_cases


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a local sample case for a single python-ai skill.")
    parser.add_argument("--skill", help="Skill name to execute.")
    parser.add_argument("--list", action="store_true", help="List available skill cases.")
    parser.add_argument("--validate", action="store_true", help="Validate the skill case registry against task handlers.")
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON output.")
    parser.add_argument("--allow-llm", action="store_true", help="Allow live LLM calls if keys are configured.")
    parser.add_argument("--keep-tempdir", action="store_true", help="Keep the generated temp directory for inspection.")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.list:
        for name, case in sorted(available_skill_cases().items()):
            print(f"{name}\t{case.description}")
        return 0

    if args.validate:
        validate_skill_cases()
        print("skill case registry ok")
        return 0

    if not args.skill:
        parser.print_help(sys.stderr)
        return 2

    try:
        result = run_skill_case(
            args.skill,
            allow_llm=args.allow_llm,
            keep_tempdir=args.keep_tempdir,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        print("available skills: " + ", ".join(sorted(available_skill_cases())), file=sys.stderr)
        return 2
    print(json.dumps(result, ensure_ascii=False, indent=2 if args.pretty else None))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
