import type { Delivery, JobEnvelope, Logger, Subscription } from "@taskrail/core";

import type { DriftConnection } from "../connection/connection.js";
import { decodeEnvelope } from "../message/envelope-codec.js";
import { readAttemptCount } from "../message/reserved.js";
import { createReceipt } from "../queue/receipt.js";
import { BrokerError, ConnectionError } from "../support/errors.js";
import { ErrorCode, MAX_FETCH_MAX } from "../wire/protocol.js";
import type { FetchedRecord } from "../wire/types.js";

const MIN_RETRY_DELAY_MS = 100;
const MAX_RETRY_DELAY_MS = 5_000;

export interface SubscriptionDeps {
  readonly subscriptionId: number;
  readonly connection: DriftConnection;
  readonly topic: string;
  readonly consumerId: string;
  readonly prefetch: number;
  readonly pollIntervalMs: number;
  readonly logger: Logger;
  readonly handler: (delivery: Delivery) => Promise<void>;
  readonly signal: AbortSignal;
  readonly ensureTopic: (topic: string) => Promise<void>;
  /** 실행할 수 없는 메시지의 처리. 성공하면 ack해서 poison message를 끊는다. */
  readonly onUndecodable: (record: FetchedRecord) => Promise<void>;
}

/**
 * FETCH 폴링 루프 하나. 미ACK 전달을 prefetch 개까지만 들고 있으므로 폴링 자체가 backpressure다.
 *
 * ack은 offset 단위다 — 브로커가 비연속 ack을 기억하고 연속 구간만 커밋 위치로 전진시킨다.
 * 따라서 어댑터가 연속 구간을 따로 모아 둘 필요가 없다.
 */
export class PollingSubscription implements Subscription {
  readonly #deps: SubscriptionDeps;
  readonly #inflight = new Map<string, JobEnvelope>();
  #loop: Promise<void> | undefined;
  #wake: (() => void) | undefined;
  #detach: () => void = () => {};
  #stopped = false;

  constructor(deps: SubscriptionDeps) {
    this.#deps = deps;
  }

  get inflight(): number {
    return this.#inflight.size;
  }

  start(): void {
    if (this.#deps.signal.aborted) {
      this.#stopped = true;
      return;
    }

    const onAbort = (): void => void this.close();
    this.#deps.signal.addEventListener("abort", onAbort, { once: true });
    this.#detach = () => this.#deps.signal.removeEventListener("abort", onAbort);

    this.#loop = this.#run();
  }

  /** 이번 전달의 envelope. 이미 확정됐거나 남의 전달이면 undefined. */
  peek(offset: string): JobEnvelope | undefined {
    return this.#inflight.get(offset);
  }

  async ack(offset: string): Promise<void> {
    if (!this.#release(offset)) return;
    await this.#deps.connection.send({
      kind: "ack",
      topic: this.#deps.topic,
      consumerId: this.#deps.consumerId,
      offset: BigInt(offset),
    });
  }

  /** ack하지 않고 슬롯만 반납한다. 브로커가 ack timeout 또는 연결 종료 시 되돌린다. */
  release(offset: string): boolean {
    return this.#release(offset);
  }

  async close(): Promise<void> {
    if (this.#stopped) return;
    this.#stopped = true;
    this.#detach();
    this.#wake?.();

    const loop = this.#loop;
    this.#loop = undefined;
    if (loop !== undefined) await loop;
  }

  #release(offset: string): boolean {
    if (!this.#inflight.delete(offset)) return false;
    this.#wake?.();
    return true;
  }

  async #run(): Promise<void> {
    let retryDelayMs = MIN_RETRY_DELAY_MS;

    while (!this.#stopped) {
      const room = this.#deps.prefetch - this.#inflight.size;
      if (room <= 0) {
        await this.#pause(this.#deps.pollIntervalMs);
        continue;
      }

      try {
        const records = await this.#fetch(room);
        retryDelayMs = MIN_RETRY_DELAY_MS;

        if (records.length === 0) {
          await this.#pause(this.#deps.pollIntervalMs);
          continue;
        }
        for (const record of records) this.#deliver(record);
      } catch (error) {
        if (this.#stopped) return;
        await this.#recover(error, retryDelayMs);
        retryDelayMs = Math.min(retryDelayMs * 2, MAX_RETRY_DELAY_MS);
      }
    }
  }

  async #fetch(room: number): Promise<readonly FetchedRecord[]> {
    const response = await this.#deps.connection.send({
      kind: "fetch",
      topic: this.#deps.topic,
      consumerId: this.#deps.consumerId,
      maxMessages: Math.min(room, MAX_FETCH_MAX),
    });

    if (response.kind !== "fetch-ok") {
      throw new ConnectionError(`FETCH에 ${response.kind} 응답이 왔다`);
    }
    return response.records;
  }

  #deliver(record: FetchedRecord): void {
    const offset = record.offset.toString();
    // 재연결 직후 같은 offset이 다시 올 수 있다. 이미 처리 중이면 두 번 실행하지 않는다.
    if (this.#inflight.has(offset)) return;

    const envelope = decodeEnvelope(record.payload);
    if (envelope === undefined) {
      void this.#poison(record, offset);
      return;
    }

    this.#inflight.set(offset, envelope);
    const delivery: Delivery = {
      envelope,
      receipt: createReceipt({
        subscriptionId: this.#deps.subscriptionId,
        topic: this.#deps.topic,
        consumerId: this.#deps.consumerId,
        offset,
      }),
      deliveryCount: readAttemptCount(record.headers),
    };

    // handler가 던지면 ack하지 않은 채로 둔다 — 재전달은 브로커가 책임진다.
    void this.#deps.handler(delivery).catch((error: unknown) => {
      this.#deps.logger.error("driftmq 전달 handler가 예외를 던졌다", {
        topic: this.#deps.topic,
        offset,
        error,
      });
    });
  }

  async #poison(record: FetchedRecord, offset: string): Promise<void> {
    try {
      await this.#deps.onUndecodable(record);
      await this.#deps.connection.send({
        kind: "ack",
        topic: this.#deps.topic,
        consumerId: this.#deps.consumerId,
        offset: record.offset,
      });
      this.#deps.logger.warn("파싱할 수 없는 메시지를 dead로 보내고 ack했다", {
        topic: this.#deps.topic,
        offset,
      });
    } catch (error) {
      this.#deps.logger.error("파싱 불가 메시지 처리 실패 — ack하지 않는다", {
        topic: this.#deps.topic,
        offset,
        error,
      });
    }
  }

  /** UNKNOWN_TOPIC은 아직 토픽이 없다는 뜻일 뿐이다 — 크래시하지 않고 만들거나 기다린다. */
  async #recover(error: unknown, delayMs: number): Promise<void> {
    if (error instanceof BrokerError && error.code === ErrorCode.UNKNOWN_TOPIC) {
      try {
        await this.#deps.ensureTopic(this.#deps.topic);
      } catch (createError) {
        this.#deps.logger.warn("토픽 생성 실패", {
          topic: this.#deps.topic,
          error: createError,
        });
      }
      await this.#pause(delayMs);
      return;
    }

    this.#deps.logger.warn("driftmq 폴링 실패 — 재시도한다", {
      topic: this.#deps.topic,
      delayMs,
      error,
    });
    await this.#pause(delayMs);
  }

  /** close나 슬롯 반납이 있으면 즉시 깨어난다. */
  #pause(ms: number): Promise<void> {
    if (this.#stopped) return Promise.resolve();

    return new Promise<void>((resolve) => {
      const done = (): void => {
        clearTimeout(timer);
        if (this.#wake === done) this.#wake = undefined;
        resolve();
      };
      const timer = setTimeout(done, ms);
      this.#wake = done;
    });
  }
}
