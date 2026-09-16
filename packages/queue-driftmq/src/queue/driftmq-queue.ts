import { TaskrailError } from "@taskrail/core";
import type {
  ConsumeOptions,
  DeadLetterReason,
  Delivery,
  DeliveryReceipt,
  JobEnvelope,
  JobQueue,
  QueueCapabilities,
  Subscription,
} from "@taskrail/core";
import { isJobEnvelope } from "@taskrail/core/internal";

import { DriftConnection } from "../connection/connection.js";
import { PollingSubscription } from "../consume/subscription.js";
import { deadHeaderValues, withoutReservedHeaders } from "../dead/dead-letter.js";
import { encodeEnvelope, toHeaders } from "../message/envelope-codec.js";
import { BrokerError } from "../support/errors.js";
import { ErrorCode } from "../wire/protocol.js";
import type { FetchedRecord, WireHeader } from "../wire/types.js";
import { resolveOptions } from "./options.js";
import type { DriftmqQueueOptions, ResolvedDriftmqOptions } from "./options.js";
import { ADAPTER_NAME, readToken } from "./receipt.js";
import { deadTopic, defaultConsumerGroup, jobTopic } from "./topics.js";

/**
 * driftmq 브로커를 목적지로 쓰는 `JobQueue`. producer용 커넥션 하나와 구독마다 커넥션 하나를
 * 쓴다 — 소켓 하나에는 in-flight 요청이 하나뿐이라 FETCH 폴링이 publish를 막으면 안 된다.
 */
export class DriftmqQueue implements JobQueue {
  readonly name = ADAPTER_NAME;

  /**
   * 지연 전달은 wire에 없다(core가 notBefore requeue로 보완한다). 재전달은 ack 부재로 일어나지만
   * 그 기한은 브로커 설정이고 핸드셰이크가 없어 알아낼 방법이 없으므로 null이다.
   * dead-letter 토픽은 브로커가 스스로 만든다.
   */
  readonly capabilities: QueueCapabilities = {
    nativeDelay: false,
    redelivery: true,
    visibilityTimeoutMs: null,
    nativeDeadLetter: true,
  };

  readonly #options: ResolvedDriftmqOptions;
  readonly #producer: DriftConnection;
  readonly #subscriptions = new Map<number, PollingSubscription>();
  readonly #consumerConnections = new Map<number, DriftConnection>();
  readonly #ensuredTopics = new Set<string>();
  #nextSubscriptionId = 1;
  #closed = false;

  constructor(options: DriftmqQueueOptions) {
    this.#options = resolveOptions(options);
    this.#producer = this.#connection("producer");
  }

  async enqueue(envelope: JobEnvelope): Promise<void> {
    this.#assertOpen();
    if (!isJobEnvelope(envelope)) {
      throw new TaskrailError("JobEnvelope 형식이 아닌 값은 enqueue할 수 없다");
    }

    const topic = jobTopic(this.#options.topicPrefix, envelope.queue);
    const { headers, payload } = encodeEnvelope(envelope);
    await this.#publish(topic, headers, payload);
  }

  async consume(
    handler: (delivery: Delivery) => Promise<void>,
    options: ConsumeOptions,
  ): Promise<Subscription> {
    this.#assertOpen();
    if (!Number.isInteger(options.prefetch) || options.prefetch < 1) {
      throw new TaskrailError("prefetch는 1 이상의 정수여야 한다");
    }

    const topic = jobTopic(this.#options.topicPrefix, options.queue);
    const dead = deadTopic(this.#options.topicPrefix, options.queue);
    await this.#ensureTopic(topic);

    const subscriptionId = this.#nextSubscriptionId++;
    const connection = this.#connection(`consumer:${topic}`);
    this.#consumerConnections.set(subscriptionId, connection);

    const subscription = new PollingSubscription({
      subscriptionId,
      connection,
      topic,
      consumerId: this.#options.consumerGroup ?? defaultConsumerGroup(options.queue),
      prefetch: options.prefetch,
      pollIntervalMs: this.#options.pollIntervalMs,
      logger: this.#options.logger,
      handler,
      signal: options.signal,
      ensureTopic: (name) => this.#ensureTopic(name),
      onUndecodable: (record) => this.#publishUndecodable(dead, topic, record),
    });

    this.#subscriptions.set(subscriptionId, subscription);
    subscription.start();

    return {
      close: async () => {
        await this.#closeSubscription(subscriptionId);
      },
    };
  }

  // ack/nack은 close 뒤에 호출돼도 던지지 않는다 — 이미 시작된 전달의 마무리 경로다.

  async ack(receipt: DeliveryReceipt): Promise<void> {
    const token = readToken(receipt);
    await this.#subscriptions.get(token.subscriptionId)?.ack(token.offset);
  }

  async nack(receipt: DeliveryReceipt, options?: { requeue?: boolean }): Promise<void> {
    const token = readToken(receipt);
    const subscription = this.#subscriptions.get(token.subscriptionId);
    if (subscription === undefined) return;

    if (options?.requeue ?? true) {
      subscription.release(token.offset);
      return;
    }

    // requeue를 거부당한 메시지는 갈 곳이 없다. 조용히 버리는 대신 dead로 보낸다.
    const envelope = subscription.peek(token.offset);
    if (envelope === undefined) return;

    await this.deadLetter(envelope, { kind: "fatal", error: "nack(requeue: false)" });
    await subscription.ack(token.offset);
  }

  async deadLetter(envelope: JobEnvelope, reason: DeadLetterReason): Promise<void> {
    const topic = deadTopic(this.#options.topicPrefix, envelope.queue);
    const { headers, payload } = encodeEnvelope(
      envelope,
      deadHeaderValues(reason, Date.now()),
    );
    await this.#publish(topic, headers, payload);
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;

    for (const subscriptionId of [...this.#subscriptions.keys()]) {
      await this.#closeSubscription(subscriptionId);
    }
    await this.#producer.close();
  }

  async #closeSubscription(subscriptionId: number): Promise<void> {
    const subscription = this.#subscriptions.get(subscriptionId);
    const connection = this.#consumerConnections.get(subscriptionId);
    this.#subscriptions.delete(subscriptionId);
    this.#consumerConnections.delete(subscriptionId);

    // 커넥션을 먼저 닫아 대기 중인 FETCH를 깨운다 — 폴링 루프가 응답을 기다리며 매달리지 않는다.
    const closing = subscription?.close();
    await connection?.close();
    await closing;
  }

  /** 파싱할 수 없는 레코드도 원본 사용자 헤더는 보존한다 — 사후 조사에 필요하다. */
  async #publishUndecodable(
    dead: string,
    sourceTopic: string,
    record: FetchedRecord,
  ): Promise<void> {
    const headers = [
      ...withoutReservedHeaders(record.headers),
      ...toHeaders(
        deadHeaderValues(
          {
            kind: "undecodable",
            detail: `${sourceTopic}@${record.offset.toString()}: JobEnvelope로 파싱할 수 없다`,
          },
          Date.now(),
        ),
      ),
    ];
    await this.#publish(dead, headers, record.payload);
  }

  async #publish(
    topic: string,
    headers: readonly WireHeader[],
    payload: Buffer,
  ): Promise<void> {
    await this.#ensureTopic(topic);

    const response = await this.#producer.send({ kind: "publish", topic, headers, payload });
    if (response.kind !== "publish-ok") {
      throw new TaskrailError(`PUBLISH에 ${response.kind} 응답이 왔다`);
    }
  }

  async #ensureTopic(topic: string): Promise<void> {
    if (!this.#options.autoCreateTopics || this.#ensuredTopics.has(topic)) return;

    try {
      await this.#producer.send({ kind: "topic-create", topic });
    } catch (error) {
      if (!(error instanceof BrokerError && error.code === ErrorCode.TOPIC_ALREADY_EXISTS)) {
        throw error;
      }
    }
    this.#ensuredTopics.add(topic);
  }

  #connection(label: string): DriftConnection {
    return new DriftConnection({
      host: this.#options.host,
      port: this.#options.port,
      connectTimeoutMs: this.#options.connectTimeoutMs,
      requestTimeoutMs: this.#options.requestTimeoutMs,
      logger: this.#options.logger,
      label,
    });
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new TaskrailError("DriftmqQueue가 이미 close()되었다");
    }
  }
}
