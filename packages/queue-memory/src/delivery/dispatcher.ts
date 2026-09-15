import type { ConsumeOptions, Delivery, JobEnvelope, Subscription } from "@taskrail/core";

import type { Channel } from "../store/channel.js";

type DeliveryHandler = (delivery: Delivery) => Promise<void>;

interface Consumer {
  readonly handler: DeliveryHandler;
  /** 동시에 미ack 상태로 보유할 수 있는 최대 전달 수. */
  readonly prefetch: number;
  outstanding: number;
  closed: boolean;
  detach: () => void;
}

/**
 * 한 채널의 consumer들에게 전달을 나눠 준다. 슬롯이 없으면 메시지를 꺼내지 않는다 —
 * prefetch가 곧 backpressure 지점이다.
 */
export class Dispatcher {
  readonly channel: Channel;
  readonly #leaseMs: number;
  readonly #consumers: Consumer[] = [];
  readonly #owners = new Map<number, Consumer>();
  #cursor = 0;
  #timer: NodeJS.Timeout | undefined;
  #closed = false;

  constructor(channel: Channel, leaseMs: number) {
    this.channel = channel;
    this.#leaseMs = leaseMs;
  }

  subscribe(handler: DeliveryHandler, options: ConsumeOptions): Subscription {
    const consumer: Consumer = {
      handler,
      prefetch: options.prefetch,
      outstanding: 0,
      closed: options.signal.aborted,
      detach: () => {},
    };

    if (!consumer.closed) {
      const onAbort = (): void => this.#closeConsumer(consumer);
      options.signal.addEventListener("abort", onAbort, { once: true });
      consumer.detach = () => options.signal.removeEventListener("abort", onAbort);
      this.#consumers.push(consumer);
      this.wake();
    }

    return {
      close: async () => {
        this.#closeConsumer(consumer);
      },
    };
  }

  wake(): void {
    this.#pump();
  }

  ack(deliveryId: number): boolean {
    if (this.channel.settle(deliveryId) === undefined) return false;
    this.#release(deliveryId);
    this.#pump();
    return true;
  }

  /** 보유 중이던 전달이었다면 그 envelope을 돌려준다(requeue하지 않을 때 호출자가 처리한다). */
  nack(deliveryId: number, requeue: boolean): JobEnvelope | undefined {
    const message = this.channel.settle(deliveryId);
    if (message === undefined) return undefined;

    this.#release(deliveryId);
    if (requeue) this.channel.requeue(message, Date.now());
    this.#pump();
    return message.envelope;
  }

  close(): void {
    this.#closed = true;
    this.#clearTimer();
    for (const consumer of this.#consumers) {
      consumer.closed = true;
      consumer.detach();
    }
    this.#consumers.length = 0;
    this.#owners.clear();
    this.channel.releaseAll(Date.now());
  }

  #pump(): void {
    if (this.#closed) return;
    this.#clearTimer();

    for (;;) {
      const consumer = this.#pickConsumer();
      if (consumer === undefined) break;

      const message = this.channel.pollDue(Date.now());
      if (message === undefined) break;

      consumer.outstanding += 1;
      const { deliveryId, delivery } = this.channel.checkout(
        message,
        this.#leaseMs,
        (expired) => this.#onLeaseExpired(expired),
      );
      this.#owners.set(deliveryId, consumer);

      // handler가 던지면 ack하지 않은 채로 둔다 — 재전달은 lease가 책임진다.
      void consumer.handler(delivery).catch(() => {});
    }

    this.#scheduleWake();
  }

  #pickConsumer(): Consumer | undefined {
    const total = this.#consumers.length;
    for (let offset = 0; offset < total; offset += 1) {
      const index = (this.#cursor + offset) % total;
      const consumer = this.#consumers[index];
      if (consumer !== undefined && !consumer.closed && consumer.outstanding < consumer.prefetch) {
        this.#cursor = (index + 1) % total;
        return consumer;
      }
    }
    return undefined;
  }

  #hasCapacity(): boolean {
    return this.#consumers.some((c) => !c.closed && c.outstanding < c.prefetch);
  }

  /** 아직 `readyAt`이 오지 않은 메시지를 깨우기 위한 타이머. 지연 전달의 본체다. */
  #scheduleWake(): void {
    if (this.#closed || this.#timer !== undefined) return;

    const readyAt = this.channel.nextReadyAt();
    if (readyAt === undefined || !this.#hasCapacity()) return;

    this.#timer = setTimeout(
      () => {
        this.#timer = undefined;
        this.#pump();
      },
      Math.max(0, readyAt - Date.now()),
    );
  }

  #onLeaseExpired(deliveryId: number): void {
    this.#release(deliveryId);
    this.#pump();
  }

  #release(deliveryId: number): void {
    const consumer = this.#owners.get(deliveryId);
    if (consumer === undefined) return;
    this.#owners.delete(deliveryId);
    consumer.outstanding = Math.max(0, consumer.outstanding - 1);
  }

  #closeConsumer(consumer: Consumer): void {
    if (consumer.closed) return;
    consumer.closed = true;
    consumer.detach();

    const index = this.#consumers.indexOf(consumer);
    if (index >= 0) this.#consumers.splice(index, 1);
    this.#cursor = 0;

    this.#pump();
  }

  #clearTimer(): void {
    if (this.#timer === undefined) return;
    clearTimeout(this.#timer);
    this.#timer = undefined;
  }
}
