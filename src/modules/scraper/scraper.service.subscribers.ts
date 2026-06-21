import { Inject, Injectable, Logger } from '@nestjs/common';
import { RabbitMQService } from '../rabbitmq/rabbitmq.service';
import { UpsertDealData } from './consumers/upsertDealData';

import { UpdateMultisigAddressConsumer } from './consumers/updateMultisigAddress';
import { GetAllowanceAuditTrailConsumer } from './consumers/getAllowanceAuditTrail';
import { ProcessSectorEventConsumer } from './consumers/processSectorEvent';
import { FetchClaimsForProviderConsumer } from './consumers/fetchClaimsForProvider';
import { ProcessClaimsFileConsumer } from './consumers/processClaimsFile';
import { ProcessClaimsBatchConsumer } from './consumers/processClaimsBatch';
import { FetchTracerVerifierAllowancesConsumer } from './consumers/fetchTracerVerifierAllowances';
import { FetchTracerVerifiedClientAllowancesConsumer } from './consumers/fetchTracerVerifiedClientAllowances';
import { FetchTracerDealsConsumer } from './consumers/fetchTracerDeals';
import { SyncTracerDataConsumer } from './consumers/syncTracerData';
import { FetchTracerVirtualVerifierAllowancesConsumer } from './consumers/fetchTracerVirtualVerifierAllowances';
import { FetchTracerVirtualVerifiedClientAllowancesConsumer } from './consumers/fetchTracerVirtualVerifiedClientAllowances';


@Injectable()
export class ScraperServiceSubscribers {
  constructor(
    protected readonly updateMultisigAddressConsumer: UpdateMultisigAddressConsumer,
    protected readonly getAllowanceAuditTrailConsumer: GetAllowanceAuditTrailConsumer,
    protected readonly upsertDealData: UpsertDealData,

    protected readonly processSectorEventConsumer: ProcessSectorEventConsumer,
    protected readonly fetchClaimsForProviderConsumer: FetchClaimsForProviderConsumer,
    protected readonly processClaimsFileConsumer: ProcessClaimsFileConsumer,
    protected readonly processClaimsBatchConsumer: ProcessClaimsBatchConsumer,
    protected readonly fetchTracerVerifierAllowancesConsumer: FetchTracerVerifierAllowancesConsumer,
    protected readonly fetchTracerVerifiedClientAllowancesConsumer: FetchTracerVerifiedClientAllowancesConsumer,
    protected readonly fetchTracerVirtualVerifierAllowancesConsumer: FetchTracerVirtualVerifierAllowancesConsumer,
    protected readonly fetchTracerVirtualVerifiedClientAllowancesConsumer: FetchTracerVirtualVerifiedClientAllowancesConsumer,
    protected readonly fetchTracerDealsConsumer: FetchTracerDealsConsumer,
    protected readonly syncTracerDataConsumer: SyncTracerDataConsumer,

    @Inject('ASYNC_RABBITMQ_CONNECTION')
    protected readonly rabbitMQService: RabbitMQService,
  ) { }

  async onModuleInit() {
    console.log(`Initialization...`);
    this.rabbitMQService.attachConsumer(
      this.processSectorEventConsumer,
      this.rabbitMQService.channel1,
    );


    this.rabbitMQService.attachConsumer(
      this.getAllowanceAuditTrailConsumer,
      this.rabbitMQService.channel1,
    );

    this.rabbitMQService.attachConsumer(
      this.upsertDealData,
      this.rabbitMQService.channel2,
    );

    this.rabbitMQService.attachConsumer(
      this.updateMultisigAddressConsumer,
      this.rabbitMQService.channel2,
    );

    this.rabbitMQService.attachConsumer(
      this.fetchClaimsForProviderConsumer,
      this.rabbitMQService.channel1,
    );

    this.rabbitMQService.attachConsumer(
      this.processClaimsFileConsumer,
      this.rabbitMQService.channel1,
    );

    this.rabbitMQService.attachConsumer(
      this.processClaimsBatchConsumer,
      this.rabbitMQService.channel1,
    );

    // Moved to channel3 to stay under Amazon MQ's 10-consumers-per-channel cap.
    this.rabbitMQService.attachConsumer(
      this.fetchTracerVerifierAllowancesConsumer,
      this.rabbitMQService.channel3,
    );

    this.rabbitMQService.attachConsumer(
      this.fetchTracerVerifiedClientAllowancesConsumer,
      this.rabbitMQService.channel3,
    );

    this.rabbitMQService.attachConsumer(
      this.fetchTracerVirtualVerifierAllowancesConsumer,
      this.rabbitMQService.channel3,
    );

    this.rabbitMQService.attachConsumer(
      this.fetchTracerVirtualVerifiedClientAllowancesConsumer,
      this.rabbitMQService.channel3,
    );

    this.rabbitMQService.attachConsumer(
      this.fetchTracerDealsConsumer,
      this.rabbitMQService.channel3,
    );

    this.rabbitMQService.attachConsumer(
      this.syncTracerDataConsumer,
      this.rabbitMQService.channel3,
    );
    console.log(`The module has been initialized.`);
  }
}
