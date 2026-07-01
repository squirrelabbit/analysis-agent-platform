import { Global, Module } from '@nestjs/common';
import { Kysely, PostgresDialect } from 'kysely';
import { Pool } from 'pg';

/**
 * DB 계층 — Go store/postgres.go(3036줄 raw SQL)를 pg + Kysely로 포팅.
 * PoC 단계라 Kysely는 sql`` raw executor로만 쓰고, 타입 스키마는 경로 확장 시 채운다.
 * Kysely 인스턴스는 DI 토큰 'DB'로 주입한다(NestJS 계층 = Go service→store 경계 대체).
 */
export const DB = 'DB';

@Global()
@Module({
  providers: [
    {
      provide: DB,
      useFactory: () => {
        const connectionString =
          process.env.DATABASE_URL ??
          'postgresql://platform:platform@127.0.0.1:15432/analysis_support';
        const dialect = new PostgresDialect({
          pool: new Pool({ connectionString, max: 10 }),
        });
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return new Kysely<any>({ dialect });
      },
    },
  ],
  exports: [DB],
})
export class DbModule {}
