// Package serviceerror — service 계층 typed error 모음.
//
// silverone 2026-05-28 — `internal/service/datasetprompts` subpackage 분리
// pilot을 위해 errors.go를 별도 패키지로 추출. 옛 `service.Err*` 호출자는 type
// alias로 호환 (`service.ErrNotFound` = `serviceerror.ErrNotFound`).
//
// http handler가 `errors.As(err, &service.ErrNotFound{})`로 typed match를
// 하기 때문에 *서비스 패키지 내 서브패키지가 같은 타입을 return*해야 매핑이
// 깨지지 않는다. alias 도입으로 외부 호출자(cmd/server, http, workflows)는
// 변경 없이 그대로 동작한다.
package serviceerror

type ErrInvalidArgument struct {
	Message string
}

func (e ErrInvalidArgument) Error() string {
	return e.Message
}

type ErrNotFound struct {
	Resource string
}

func (e ErrNotFound) Error() string {
	if e.Resource == "" {
		return "resource not found"
	}
	return e.Resource + " not found"
}

type ErrConflict struct {
	Message string
}

func (e ErrConflict) Error() string {
	return e.Message
}
