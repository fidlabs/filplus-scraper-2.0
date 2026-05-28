import { Injectable } from '@nestjs/common';
import { Cron, CronExpression } from '@nestjs/schedule';

import { AppConfig } from '../../configuration/configuration.service';

import { CronUtilsService, EnhancedCron } from './cronUtils.service';

@Injectable()
export class ScraperServiceCrons {
  constructor(
    private cronUtilsService: CronUtilsService,

    protected readonly config: AppConfig,
  ) { }

  @EnhancedCron(
    CronExpression.EVERY_30_MINUTES,
    CronExpression.EVERY_30_MINUTES,
  )
  async updateMultisigAddress() {
    await this.cronUtilsService.executeCron(
      'updateMultisigAddress',
      'scraperService',
      [],
    );
  }

  @EnhancedCron(
    CronExpression.EVERY_30_MINUTES,
    CronExpression.EVERY_30_MINUTES,
  )
  async updateAllowances() {
    await this.cronUtilsService.executeCron(
      'updateAllowancesFromDataCapActor',
      'scraperService',
      [],
    );
  }

  @EnhancedCron(
    CronExpression.EVERY_30_MINUTES,
    CronExpression.EVERY_30_MINUTES,
  )
  async updateAllowancesFromMetaAllocator() {
    await this.cronUtilsService.executeCron(
      'updateAllowancesFromMetaAllocator',
      'scraperService',
      [],
    );
  }

  @EnhancedCron(
    CronExpression.EVERY_12_HOURS,
    CronExpression.EVERY_12_HOURS,
  )
  async getMetaAllocatorsFromFactory() {
    await this.cronUtilsService.executeCron(
      'getMetaAllocatorsFromFactory',
      'scraperService',
      [],
    );
  }

  @EnhancedCron(
    CronExpression.EVERY_DAY_AT_11AM,
    CronExpression.EVERY_DAY_AT_11AM,
  )
  async refreshMinerList() {
    await this.cronUtilsService.executeCron(
      'refreshMinerList',
      'scraperService',
      [],
    );
  }

  // @EnhancedCron(CronExpression.EVERY_MINUTE, CronExpression.EVERY_MINUTE)
  async getSectorEvents() {
    await this.cronUtilsService.executeCron(
      'getSectorEvents',
      'scraperService',
      [],
    );
  }

  @EnhancedCron(CronExpression.EVERY_2_HOURS, CronExpression.EVERY_2_HOURS)
  async buildNetiDashboardGraphs() {
    await this.cronUtilsService.executeCron(
      'buildNetiDashboardGraphs',
      'scraperService',
      [],
    );
  }

  @EnhancedCron(
    CronExpression.EVERY_4_HOURS,
    CronExpression.EVERY_4_HOURS,
  )
  async syncAllTracerData() {
    await this.cronUtilsService.executeCron(
      'syncAllTracerData',
      'scraperService',
      [],
    );
  }
}
