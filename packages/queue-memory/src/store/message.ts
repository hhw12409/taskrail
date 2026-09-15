import type { JobEnvelope } from "@taskrail/core";

/** 큐에 들어 있는 한 통의 메시지. 재전달돼도 같은 객체가 유지된다. */
export interface QueuedMessage {
  readonly id: number;
  readonly envelope: JobEnvelope;
  /** 이 시각 전에는 전달되지 않는다. 재전달 시 즉시 전달 가능해진다. */
  readyAt: number;
  deliveries: number;
}
