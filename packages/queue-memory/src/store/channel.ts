import type { Delivery, JobEnvelope } from "@taskrail/core";

import { createReceipt } from "../queue/receipt.js";
import type { QueuedMessage } from "./message.js";
import { ReadyQueue } from "./ready-queue.js";

export interface Checkout {
  readonly deliveryId: number;
  readonly delivery: Delivery;
}

interface Lease {
  readonly message: QueuedMessage;
  readonly timer: NodeJS.Timeout;
}

/**
 * 큐 이름 하나의 저장 상태: 전달 대기(ready) + 미ack 보유(in-flight).
 * lease가 만료되면 브로커처럼 스스로 ready로 되돌린다 — 이게 crash 복구의 축소판이다.
 */
export class Channel {
  readonly name: string;
  readonly #ready = new ReadyQueue();
  readonly #leases = new Map<number, Lease>();
  #nextMessageId = 1;
  #nextDeliveryId = 1;

  constructor(name: string) {
    this.name = name;
  }

  get readySize(): number {
    return this.#ready.size;
  }

  get inflightSize(): number {
    return this.#leases.size;
  }

  offer(envelope: JobEnvelope): void {
    this.#ready.push({
      id: this.#nextMessageId++,
      envelope,
      readyAt: envelope.notBefore,
      deliveries: 0,
    });
  }

  nextReadyAt(): number | undefined {
    return this.#ready.peek()?.readyAt;
  }

  pollDue(now: number): QueuedMessage | undefined {
    const head = this.#ready.peek();
    if (head === undefined || head.readyAt > now) return undefined;
    return this.#ready.pop();
  }

  checkout(
    message: QueuedMessage,
    leaseMs: number,
    onLeaseExpired: (deliveryId: number) => void,
  ): Checkout {
    const deliveryId = this.#nextDeliveryId++;
    message.deliveries += 1;

    const timer = setTimeout(() => {
      this.#leases.delete(deliveryId);
      this.requeue(message, Date.now());
      onLeaseExpired(deliveryId);
    }, leaseMs);

    this.#leases.set(deliveryId, { message, timer });

    return {
      deliveryId,
      delivery: {
        envelope: message.envelope,
        receipt: createReceipt({
          queue: this.name,
          messageId: message.id,
          deliveryId,
        }),
        deliveryCount: message.deliveries,
      },
    };
  }

  /** 이미 확정됐거나 lease가 만료된 전달이면 undefined — 늦은 ack/nack은 무시된다. */
  settle(deliveryId: number): QueuedMessage | undefined {
    const lease = this.#leases.get(deliveryId);
    if (lease === undefined) return undefined;
    clearTimeout(lease.timer);
    this.#leases.delete(deliveryId);
    return lease.message;
  }

  requeue(message: QueuedMessage, now: number): void {
    message.readyAt = now;
    this.#ready.push(message);
  }

  /** close 시 타이머를 남기지 않는다. 보유 중이던 전달은 ready로 되돌린다. */
  releaseAll(now: number): void {
    for (const lease of this.#leases.values()) {
      clearTimeout(lease.timer);
      this.requeue(lease.message, now);
    }
    this.#leases.clear();
  }
}
