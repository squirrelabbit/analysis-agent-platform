package service

import "analysis-support-platform/control-plane/internal/serviceerror"

// silverone 2026-05-28 — service.Err* 는 `internal/serviceerror`로 이동했다.
// 외부 호출자(http handler, cmd/server)는 `service.ErrNotFound` 그대로 사용
// 가능하도록 type alias로 호환을 유지한다. subpackage(datasetprompts 등)는
// 직접 `serviceerror.ErrNotFound`를 return해서 순환 import를 피한다.
type (
	ErrInvalidArgument = serviceerror.ErrInvalidArgument
	ErrNotFound        = serviceerror.ErrNotFound
	ErrConflict        = serviceerror.ErrConflict
	ErrUnauthorized    = serviceerror.ErrUnauthorized
	ErrForbidden       = serviceerror.ErrForbidden
)
