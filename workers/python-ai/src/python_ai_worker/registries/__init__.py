"""Registry / policy / prompt modules.

δ-4 (5/21)로 ``skill_bundle`` submodule이 삭제됐다 — plan은 planner가
LLM으로 생성하므로 고정 카탈로그가 필요하지 않다. 남은 submodule은
``task_registry`` (internal task 카탈로그), ``prompt`` (prompt 4-tier
resolver), ``policy`` (issue_evidence_summary policy 잔재).

Submodules는 eager import 하지 않는다 — runtime이 config를 import 하고
config가 prompt registry를 import하는 순환 의존 가능성을 피하기 위해
호출자가 명시적으로 ``from python_ai_worker.registries import prompt`` 같은
형태로 import한다.
"""
