import { Global, Module } from '@nestjs/common';
import { Kysely, PostgresDialect } from 'kysely';
import { Pool, types } from 'pg';

/**
 * DB 계층 — Go store/postgres.go(3036줄 raw SQL)를 pg + Kysely로 포팅.
 * PoC 단계라 Kysely는 sql`` raw executor로만 쓰고, 타입 스키마는 경로 확장 시 채운다.
 * Kysely 인스턴스는 DI 토큰 'DB'로 주입한다(NestJS 계층 = Go service→store 경계 대체).
 */
export const DB = 'DB';

// timestamptz(OID 1184)를 JS Date로 파싱하지 않고 raw text로 받는다.
// Date는 ms 정밀도라 µs가 유실돼 Go(RFC3339Nano) 응답과 어긋난다 —
// common/go-time.ts goTimestamptz가 text를 Go 포맷으로 변환한다.
types.setTypeParser(1184, (value: string) => value);

@Global()
@Module({
  providers: [
    {
      provide: DB,
      useFactory: () => {
        const connectionString =
          process.env.DATABASE_URL ??
          'postgresql://platform:platform@127.0.0.1:15432/analysis_support';
        // 세션 timezone은 서버 기본값 유지 — Go(lib/pq)도 세션 offset 그대로 marshal
        // 하므로 여기서 SET TIME ZONE을 하면 계약(offset 표기)이 어긋난다.
        const pool = new Pool({ connectionString, max: 10 });
        const dialect = new PostgresDialect({ pool });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return new Kysely<any>({ dialect });
      },
    },
  ],
  exports: [DB],
})
export class DbModule {}
