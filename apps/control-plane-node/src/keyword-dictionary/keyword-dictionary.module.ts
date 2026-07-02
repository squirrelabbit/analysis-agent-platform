import { Module } from '@nestjs/common';
import { KeywordDictionaryController } from './keyword-dictionary.controller';
import { KeywordDictionaryRepository } from './keyword-dictionary.repository';
import { KeywordDictionaryService } from './keyword-dictionary.service';

@Module({
  controllers: [KeywordDictionaryController],
  providers: [KeywordDictionaryService, KeywordDictionaryRepository],
  exports: [KeywordDictionaryRepository],
})
export class KeywordDictionaryModule {}
