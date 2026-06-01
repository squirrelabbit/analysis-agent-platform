from __future__ import annotations

"""dataset_build entry point re-export shell.

(β2 / 5/19) — dataset_document_cluster_profile 추가 제거 후 3 task만 보존:
clean, doc_genuineness, clause_label.

외부 import 경로 `from python_ai_worker.dataset_build import run_dataset_clean`
호환을 위해 여기서 re-export. 일부 underscored helper와 ``rt`` namespace는
기존 테스트가 `python_ai_worker.dataset_build.rt....` 또는 underscored 이름으로
직접 import / patch하므로 호환을 위해 함께 노출.
"""

from .. import runtime as rt  # 테스트 호환: python_ai_worker.dataset_build.rt 경로

from .clause_label import run_dataset_clause_label
from .clean import (
    _apply_noise_scrub,
    _load_noise_patterns,
    run_dataset_clean,
)
from .doc_genuineness import run_dataset_doc_genuineness

__all__ = [
    "run_dataset_clause_label",
    "run_dataset_clean",
    "run_dataset_doc_genuineness",
]
