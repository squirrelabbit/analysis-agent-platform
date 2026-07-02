import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule } from './app.module';

/**
 * control-plane Node PoC 엔트리. strangler 단계라 Go(:8080/:18080)와 별도 포트로 뜬다.
 * 컷오버 전엔 reverse-proxy가 포팅된 경로만 이쪽으로 보낸다.
 */
async function bootstrap(): Promise<void> {
  const app = await NestFactory.create(AppModule);
  const port = Number(process.env.PORT ?? 18081);
  const host = process.env.HOST ?? '0.0.0.0';
  await app.listen(port, host);
  // eslint-disable-next-line no-console
  console.log(`control-plane-node PoC listening on http://${host}:${port}`);
}

void bootstrap();
