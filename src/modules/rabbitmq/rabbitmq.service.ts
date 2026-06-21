import * as amqplib from 'amqplib';

import { Logger } from '@nestjs/common';
import { Channel } from 'amqplib';
import { ConnectionOptions, IConsumer } from './rabbitmq.types';

export class RabbitMQService {
  public channel1: amqplib.Channel;
  public channel2: amqplib.Channel;
  connection: amqplib.Connection;
  connectionOptions: ConnectionOptions;

  constructor(connectionOptions: ConnectionOptions) {
    this.connectionOptions = connectionOptions;
  }

  public async connect() {
    const { hostname, username, password, frameMax } = this.connectionOptions;
    // Defaults preserve local/plaintext dev (amqp://...:5672). For Amazon MQ,
    // set RABBITMQ_PROTOCOL=amqps and RABBITMQ_PORT=5671 to connect over TLS.
    this.connection = await amqplib.connect({
      protocol: process.env.RABBITMQ_PROTOCOL || 'amqp',
      hostname,
      port: process.env.RABBITMQ_PORT ? parseInt(process.env.RABBITMQ_PORT, 10) : 5672,
      username,
      password,
      frameMax,
    });
    this.channel1 = undefined;
  }

  public async setup() {
    await this.declareChannels();
    await this.declareExchanges('scraper', 'direct');

    await this.declareQueue(
      this.channel1,
      'scraper',
      'processSectorEvent',
      'processSectorEvent',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'getAllowanceAuditTrail',
      'getAllowanceAuditTrail',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'upsertDealData',
      'upsertDealData',
    );

    await this.declareQueue(
      this.channel2,
      'scraper',
      'updateMultisigAddress',
      'updateMultisigAddress',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'fetchClaimsForProvider',
      'fetchClaimsForProvider',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'processClaimsFile',
      'processClaimsFile',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'processClaimsBatch',
      'processClaimsBatch',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'fetchTracerVerifierAllowances',
      'fetchTracerVerifierAllowances',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'fetchTracerVerifiedClientAllowances',
      'fetchTracerVerifiedClientAllowances',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'fetchTracerVirtualVerifierAllowances',
      'fetchTracerVirtualVerifierAllowances',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'fetchTracerVirtualVerifiedClientAllowances',
      'fetchTracerVirtualVerifiedClientAllowances',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'fetchTracerDeals',
      'fetchTracerDeals',
    );

    await this.declareQueue(
      this.channel1,
      'scraper',
      'syncTracerData',
      'syncTracerData',
    );
  }

  public async declareExchanges(
    exchangeName: string,
    type: 'direct' | 'topic' | 'headers' | 'fanout' | 'match',
  ) {
    await this.channel1.assertExchange(`${exchangeName}`, type);
    await this.channel1.assertExchange(`${exchangeName}-dlx`, type);
  }

  public async declareQueue(
    channel: Channel,
    exchange: string,
    queue: string,
    routingKey: string,
  ) {
    await channel.assertQueue(queue);
    await channel.assertQueue(`${queue}.dlx`, {
      messageTtl: 1000,
      deadLetterExchange: exchange,
    });
    await channel.bindQueue(queue, exchange, routingKey);
    await channel.bindQueue(`${queue}.dlx`, `${exchange}-dlx`, routingKey);
    Logger.log(`[RabbitMQ] queue ${queue} binded to exchange ${exchange}`);
  }

  public async publish(exchange: string, routingKey: string, msg: string) {
    this.channel1.publish(exchange, routingKey, Buffer.from(msg));
  }

  public async retry(exchange: string, routingKey: string, msg: string) {
    this.channel1.publish(`${exchange}-dlx`, routingKey, Buffer.from(msg));
  }

  public async attachConsumer(consumer: IConsumer, channel: amqplib.Channel) {
    //const channel = this.channel1;
    await channel.assertQueue(consumer.queue);
    await channel.consume(
      consumer.queue,
      (msg) => {
        if (msg !== null) {
          consumer.exec(msg.content.toString(), channel, msg);
        }
      },
      { noAck: false },
    );
    Logger.log(
      `[RabbitMQ::attachConsumer] Consumer attached to queue ${consumer.queue}`,
    );
  }

  private onChannelClose() {
    console.log(`[RabbitMQ::onChannelClose]`);
  }

  private onChannelError(error) {
    console.log(`[RabbitMQ::onChannelError] ${error}`);
  }

  public async close() {
    await this.connection.close();

    this.connection = undefined;
    this.channel1 = undefined;
  }

  private async declareChannels() {
    this.channel1 = await this.connection.createChannel();
    await this.channel1.prefetch(1);
    this.channel1.on('error', this.onChannelError);
    this.channel1.on('close', this.onChannelClose);

    this.channel2 = await this.connection.createChannel();
    await this.channel2.prefetch(20);
    this.channel2.on('error', this.onChannelError);
    this.channel2.on('close', this.onChannelClose);
  }
}
