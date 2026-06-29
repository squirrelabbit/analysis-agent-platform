from __future__ import annotations

"""실행 중인 dataset build task의 협조적 취소 레지스트리 (silverone 2026-06-29).

control-plane이 `POST /tasks/cancel {dataset_version_id}`를 보내면 해당 version의 실행 중
build task가 든 threading.Event를 set한다. task는 메인 루프에서 event.is_set()를 확인해
남은 처리를 멈추고 거기까지의 결과를 flush한 뒤 정상 반환한다(summary.cancelled=True).

key = dataset_version_id. 파이프라인은 버전당 순차 실행이라 버전당 동시 빌드는 1개라는
전제(같은 버전 2빌드 동시 실행은 미지원). 단일 worker 프로세스 in-memory 레지스트리.
"""

import threading

_LOCK = threading.Lock()
_EVENTS: dict[str, threading.Event] = {}


def begin(key: str) -> threading.Event:
    """key(version_id)로 취소 Event를 등록하고 반환한다. 이미 있으면 재사용(clear)."""
    key = str(key or "").strip()
    with _LOCK:
        event = _EVENTS.get(key)
        if event is None:
            event = threading.Event()
            _EVENTS[key] = event
        else:
            event.clear()
        return event


def request(key: str) -> bool:
    """key의 실행 중 task에 취소를 요청한다. 등록된 게 있으면 set 후 True, 없으면 False."""
    key = str(key or "").strip()
    with _LOCK:
        event = _EVENTS.get(key)
        if event is None:
            return False
        event.set()
        return True


def end(key: str) -> None:
    """task 종료 시 레지스트리에서 제거(finally에서 호출)."""
    key = str(key or "").strip()
    with _LOCK:
        _EVENTS.pop(key, None)
