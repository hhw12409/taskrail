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

import { DeadLetterStore } from "../dead/dead-letter-store.js";
import type { DeadLetterRecord } from "../dead/dead-letter-store.js";
import { Dispatcher } from "../delivery/dispatcher.js";
import { Channel } from "../store/channel.js";
import { ADAPTER_NAME, readToken } from "./receipt.js";

export const DEFAULT_VISIBILITY_TIMEOUT_MS = 30_000;

export interface MemoryQueueOptions {
  /** in-flight 전달의 lease 길이. 만료되면 재전달한다. */
  visibilityTimeoutMs?: number;
  /** 큐당 최대 대기 메시지 수. 초과하면 enqueue가 throw. */
  maxQueueSize?: number;
}

export interface QueueStats {
  readonly ready: number;
  readonly inflight: number;
}

/**
 * 프로세스 안에서 도는 `JobQueue` 구현. 지연 전달과 lease 재전달을 실제로 수행하므로
 * driftmq 없이도 코어의 상태 머신을 그대로 돌릴 수 있다.
 */
export class MemoryQueue implements JobQueue {
  readonly name = ADAPTER_NAME;
  readonly capabilities: QueueCapabilities;

  readonly #visibilityTimeoutMs: number;
  readonly #maxQueueSize: number;
  readonly #dispatchers = new Map<string, Dispatcher>();
  readonly #dead = new DeadLetterStore();
  #closed = false;

  constructor(options?: MemoryQueueOptions) {
    const visibilityTimeoutMs =
      options?.visibilityTimeoutMs ?? DEFAULT_VISIBILITY_TIMEOUT_MS;
    if (!Number.isFinite(visibilityTimeoutMs) || visibilityTimeoutMs <= 0) {
      throw new TaskrailError("visibilityTimeoutMs는 0보다 큰 유한한 숫자여야 한다");
    }

    const maxQueueSize = options?.maxQueueSize ?? Number.POSITIVE_INFINITY;
    if (maxQueueSize !== Number.POSITIVE_INFINITY) {
      if (!Number.isInteger(maxQueueSize) || maxQueueSize < 1) {
        throw new TaskrailError("maxQueueSize는 1 이상의 정수여야 한다");
      }
    }

    this.#visibilityTimeoutMs = visibilityTimeoutMs;
    this.#maxQueueSize = maxQueueSize;
    this.capabilities = {
      nativeDelay: true,
      redelivery: true,
      visibilityTimeoutMs,
      nativeDeadLetter: false,
    };
  }

  async enqueue(envelope: JobEnvelope): Promise<void> {
    this.#assertOpen();
    if (!isJobEnvelope(envelope)) {
      throw new TaskrailError("JobEnvelope 형식이 아닌 값은 enqueue할 수 없다");
    }

    const dispatcher = this.#dispatcherFor(envelope.queue);
    if (dispatcher.channel.readySize >= this.#maxQueueSize) {
      throw new TaskrailError(
        `큐 "${envelope.queue}"가 maxQueueSize(${this.#maxQueueSize})에 도달했다`,
      );
    }

    dispatcher.channel.offer(envelope);
    dispatcher.wake();
  }

  async consume(
    handler: (delivery: Delivery) => Promise<void>,
    options: ConsumeOptions,
  ): Promise<Subscription> {
    this.#assertOpen();
    if (!Number.isInteger(options.prefetch) || options.prefetch < 1) {
      throw new TaskrailError("prefetch는 1 이상의 정수여야 한다");
    }
    return this.#dispatcherFor(options.queue).subscribe(handler, options);
  }

  // ack/nack/deadLetter는 close 뒤에도 거부하지 않는다 — 이미 시작된 전달의 마무리 경로다.

  async ack(receipt: DeliveryReceipt): Promise<void> {
    const token = readToken(receipt);
    this.#dispatchers.get(token.queue)?.ack(token.deliveryId);
  }

  async nack(receipt: DeliveryReceipt, options?: { requeue?: boolean }): Promise<void> {
    const token = readToken(receipt);
    const requeue = options?.requeue ?? true;

    const envelope = this.#dispatchers.get(token.queue)?.nack(token.deliveryId, requeue);

    // requeue를 거부당한 메시지는 갈 곳이 없다. 조용히 버리는 대신 dead로 보낸다.
    if (envelope !== undefined && !requeue) {
      this.#dead.add(envelope, { kind: "fatal", error: "nack(requeue: false)" }, Date.now());
    }
  }

  async deadLetter(envelope: JobEnvelope, reason: DeadLetterReason): Promise<void> {
    this.#dead.add(envelope, reason, Date.now());
  }

  async close(): Promise<void> {
    if (this.#closed) return;
    this.#closed = true;
    for (const dispatcher of this.#dispatchers.values()) {
      dispatcher.close();
    }
  }

  deadLetters(): readonly DeadLetterRecord[] {
    return this.#dead.list();
  }

  stats(queue: string): QueueStats {
    const channel = this.#dispatchers.get(queue)?.channel;
    return {
      ready: channel?.readySize ?? 0,
      inflight: channel?.inflightSize ?? 0,
    };
  }

  #dispatcherFor(queue: string): Dispatcher {
    const existing = this.#dispatchers.get(queue);
    if (existing !== undefined) return existing;

    const created = new Dispatcher(new Channel(queue), this.#visibilityTimeoutMs);
    this.#dispatchers.set(queue, created);
    return created;
  }

  #assertOpen(): void {
    if (this.#closed) {
      throw new TaskrailError("MemoryQueue가 이미 close()되었다");
    }
  }
}
