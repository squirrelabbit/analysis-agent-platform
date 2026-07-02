import { HttpException } from '@nestjs/common';

/**
 * Go writeError는 `{"detail": "<message>"}` 단일 shape로 응답한다.
 * NestJS 기본 에러 body(statusCode/message)와 다르므로 HttpException에
 * detail body를 직접 실어 Go 계약을 유지한다.
 */
export function httpError(status: number, message: string): HttpException {
  return new HttpException({ detail: message.trim() }, status);
}

/** Go serviceerror.ErrNotFound{Resource: resource} → 404 `"<resource> not found"`. */
export function notFound(resource: string): HttpException {
  return httpError(404, `${resource} not found`);
}
