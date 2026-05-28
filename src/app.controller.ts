import { Controller, Get, UseGuards } from '@nestjs/common';
import { EventEmitter2 } from '@nestjs/event-emitter';
import { AppService } from './app.service';
import { ScraperService } from './modules/scraper/scraper.service';
import { ScraperServiceCrons } from './modules/scraper/crons/scraper.service.crons';
import { GhScraperServiceCrons } from './modules/scraper/crons/gh-scraper.service.crons';
import { ApiService } from './modules/api/api.service';
import { GithubApiScraper } from './modules/scraper/github/GithubApiScraper';
import { AdminApiKeyGuard } from './modules/auth/adminApiKey.guard';
import { ApiExcludeEndpoint } from '@nestjs/swagger';

@Controller()
@UseGuards(AdminApiKeyGuard)
export class AppController {
  constructor(
    private readonly appService: AppService,
    private readonly apiService: ApiService,
    private readonly scraperService: ScraperService,
    private readonly scraperServiceCrons: ScraperServiceCrons,
    private readonly githubApi: GithubApiScraper,
  ) { }

  //api endpoints to trigger scraper service crons

  @ApiExcludeEndpoint()
  @Get('/util/triggerUpdateMultisigAddress')
  async triggerUpdateMultisigAddress(): Promise<string> {
    await this.scraperServiceCrons.updateMultisigAddress();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/triggerUpdateAllowances')
  async triggerUpdateAllowances(): Promise<any> {
    return await this.scraperServiceCrons.updateAllowances();
  }

  @ApiExcludeEndpoint()
  @Get('/util/updateAllowancesFromDataCapActor')
  async updateAllowancesFromDataCapActor(): Promise<string> {
    await this.scraperServiceCrons.updateAllowances();
    return 'done';
  }

  //scraper endpoints to trigger various scrapes and data processing
  @ApiExcludeEndpoint()
  @Get('/util/syncTracerVerifierAllowances')
  async syncTracerVerifierAllowances(): Promise<string> {
    await this.scraperService.syncTracerVerifierAllowances();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/syncTracerVirtualVerifierAllowances')
  async syncTracerVirtualVerifierAllowances(): Promise<string> {
    await this.scraperService.syncTracerVirtualVerifierAllowances();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/syncTracerVerifiedClientAllowances')
  async syncTracerVerifiedClientAllowances(): Promise<string> {
    await this.scraperService.syncTracerVerifiedClientAllowances();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/syncTracerVirtualVerifiedClientAllowances')
  async syncTracerVirtualVerifiedClientAllowances(): Promise<string> {
    await this.scraperService.syncTracerVirtualVerifiedClientAllowances();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/syncTracerDeals')
  async syncTracerDeals(): Promise<string> {
    await this.scraperService.syncTracerDeals();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/syncAllTracerData')
  async syncAllTracerData(): Promise<string> {
    await this.scraperService.syncAllTracerData();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/getMetaAllocatorsFromFactory')
  async getMetaAllocatorsFromFactory(): Promise<string> {
    await this.scraperService.getMetaAllocatorsFromFactory();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/getAllowanceAuditTrail')
  async getAllowanceAuditTrail(): Promise<string> {
    await this.scraperService.getAllowanceAuditTrail();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/updateUnifiedDealsWithNewClaims')
  async updateUnifiedDealsWithNewClaims(): Promise<any> {
    // await this.scraperService.updateUnifiedDealsWithNewClaims();
  }

  @ApiExcludeEndpoint()
  @Get('/util/refreshMinerList')
  async refreshMinerList(): Promise<any> {
    await this.scraperService.refreshMinerList();
  }

  @ApiExcludeEndpoint()
  @Get('/util/getSectorEvents')
  async getSectorEvents(): Promise<any> {
    await this.scraperService.getSectorEvents();
  }

  @ApiExcludeEndpoint()
  @Get('/util/getGhApiLimits')
  async getGhApiLimits(): Promise<any> {
    return await this.scraperService.getGhApiLimits();
  }

  @ApiExcludeEndpoint()
  @Get('/util/updateAllowancesFromMetaAllocator')
  async updateAllowancesFromMetaAllocator(): Promise<string> {
    await this.scraperService.updateAllowancesFromMetaAllocator();
    return 'done';
  }

  @ApiExcludeEndpoint()
  @Get('/util/buildNetiDashboardGraphs')
  async buildNetiDashboardGraphs(): Promise<string> {
    await this.scraperService.buildNetiDashboardGraphs();
    return 'done';
  }

  //github api related endpoints

  @ApiExcludeEndpoint()
  @Get('/util/getAllocatorRegistryContents')
  async getAllocatorRegistryContents(): Promise<any> {
    await this.githubApi.getAllocatorRegistryContents();
  }

  @ApiExcludeEndpoint()
  @Get('/util/getAllocationsFromContents')
  async getAllocationsFromContents(): Promise<any> {
    await this.githubApi.getAllocationsFromContents();
  }

  @ApiExcludeEndpoint()
  @Get('/util/fetchRateLimit')
  async fetchRateLimit(): Promise<any> {
    return await this.githubApi.fetchRateLimit();
  }

}
