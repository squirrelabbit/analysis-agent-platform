import { Controller, Get, Inject } from '@nestjs/common';
import { Kysely, sql } from 'kysely';
import { DB } from '../db/db.module';

/** PoC de-risk: Node 기동 + Postgres 왕복 확인용 헬스체크. */
@Controller('healthz')
export class HealthController {
  constructor(@Inject(DB) private readonly db: Kysely<any>) {}

  @Get()
  async check(): Promise<{ status: string; db: string }> {
    try {
      await sql`SELECT 1`.execute(this.db);
      return { status: 'ok', db: 'up' };
    } catch {
      return { status: 'degraded', db: 'down' };
    }
  }
}
