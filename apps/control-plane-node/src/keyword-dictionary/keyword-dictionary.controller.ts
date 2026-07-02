import { Controller, Get, Param, Query } from '@nestjs/common';
import {
  KeywordDictionaryEventListResponse,
  KeywordDictionaryRuleListResponse,
} from './keyword-dictionary.dto';
import { KeywordDictionaryService } from './keyword-dictionary.service';

@Controller('projects/:project_id/datasets/:dataset_id/keyword_dictionary')
export class KeywordDictionaryController {
  constructor(private readonly service: KeywordDictionaryService) {}

  /** GET .../keyword_dictionary — Go handleListKeywordDictionary 계약 동일.
   *  ?include_inactive=<any>면 비활성 규칙 포함(이력 화면용). 기본은 활성만. */
  @Get()
  async list(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
    @Query('include_inactive') includeInactive?: string,
  ): Promise<KeywordDictionaryRuleListResponse> {
    const activeOnly = (includeInactive ?? '').trim() === '';
    return this.service.listRules(projectId, datasetId, activeOnly);
  }

  /** GET .../keyword_dictionary/history — Go handleListKeywordDictionaryHistory 계약 동일. */
  @Get('history')
  async history(
    @Param('project_id') projectId: string,
    @Param('dataset_id') datasetId: string,
  ): Promise<KeywordDictionaryEventListResponse> {
    return this.service.listEvents(projectId, datasetId);
  }
}
