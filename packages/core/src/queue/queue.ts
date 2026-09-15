import type { JobEnvelope } from "./envelope.js";

/** core가 브로커에 대해 아는 것은 이 인터페이스 하나다. */
export interface JobQueue {
  readonly name: string;
  readonly capabilities: QueueCapabilities;

  enqueue(envelope: JobEnvelope): Promise<void>;

  consume(
    handler: (delivery: Delivery) => Promise<void>,
    options: ConsumeOptions,
  ): Promise<Subscription>;

  ack(receipt: DeliveryReceipt): Promise<void>;
  nack(receipt: DeliveryReceipt, options?: { requeue?: boolean }): Promise<void>;

  deadLetter(envelope: JobEnvelope, reason: DeadLetterReason): Promise<void>;

  close(): Promise<void>;
}

export interface QueueCapabilities {
  /** false면 core가 notBefore 기반 requeue 전략으로 보완한다. */
  readonly nativeDelay: boolean;
  /** false면 worker crash 복구가 불가능하다. */
  readonly redelivery: boolean;
  /** 모르면 null. */
  readonly visibilityTimeoutMs: number | null;
  /** 브로커 자체 DLQ 보유 여부. Taskrail의 dead 목적지와는 별개다. */
  readonly nativeDeadLetter: boolean;
}

export interface ConsumeOptions {
  readonly queue: string;
  /** 동시에 미ack 상태로 보유할 최대 전달 수. */
  readonly prefetch: number;
  readonly signal: AbortSignal;
}

export interface Subscription {
  close(): Promise<void>;
}

export interface Delivery {
  readonly envelope: JobEnvelope;
  readonly receipt: DeliveryReceipt;
  /** 브로커가 알려주는 전달 회차. 모르면 1. */
  readonly deliveryCount: number;
}

/** 어댑터 내부 값(예: driftmq offset). core는 내용을 들여다보지 않는다. */
export interface DeliveryReceipt {
  readonly adapter: string;
  readonly token: unknown;
}

export type DeadLetterReason =
  | { kind: "attempts-exhausted"; attempt: number; error: string }
  | { kind: "fatal"; error: string }
  | { kind: "unknown-job"; name: string }
  | { kind: "undecodable"; detail: string };
