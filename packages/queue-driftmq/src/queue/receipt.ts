import { TaskrailError } from "@taskrail/core";
import type { DeliveryReceipt } from "@taskrail/core";

export const ADAPTER_NAME = "driftmq";

/**
 * ack 단위는 논리 job이 아니라 (topic, consumerId, offset)이다. 같은 jobId가 동시에 두 번
 * in-flight일 수 있으므로 offset만이 이번 전달을 가리킨다. offset은 i64라 문자열로 담는다.
 */
export interface DriftmqToken {
  readonly subscriptionId: number;
  readonly topic: string;
  readonly consumerId: string;
  readonly offset: string;
}

export function createReceipt(token: DriftmqToken): DeliveryReceipt {
  return { adapter: ADAPTER_NAME, token };
}

export function readToken(receipt: DeliveryReceipt): DriftmqToken {
  if (receipt === null || typeof receipt !== "object") {
    throw new TaskrailError("DeliveryReceipt가 아니다");
  }
  if (receipt.adapter !== ADAPTER_NAME) {
    throw new TaskrailError(
      `"${receipt.adapter}" 어댑터의 receipt를 driftmq 큐에 줄 수 없다`,
    );
  }

  const token = receipt.token as Partial<DriftmqToken> | null | undefined;
  if (
    token === null ||
    typeof token !== "object" ||
    typeof token.subscriptionId !== "number" ||
    typeof token.topic !== "string" ||
    typeof token.consumerId !== "string" ||
    typeof token.offset !== "string"
  ) {
    throw new TaskrailError("driftmq receipt의 token 형식이 올바르지 않다");
  }

  return {
    subscriptionId: token.subscriptionId,
    topic: token.topic,
    consumerId: token.consumerId,
    offset: token.offset,
  };
}
