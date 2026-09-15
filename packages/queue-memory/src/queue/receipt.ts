import { TaskrailError } from "@taskrail/core";
import type { DeliveryReceipt } from "@taskrail/core";

export const ADAPTER_NAME = "memory";

/**
 * `deliveryId`는 전달 한 번마다 새로 발급된다. lease가 만료돼 재전달된 뒤 도착한 늦은 ack이
 * 새 전달을 잘못 확정하는 것을 막는다.
 */
export interface MemoryToken {
  readonly queue: string;
  readonly messageId: number;
  readonly deliveryId: number;
}

export function createReceipt(token: MemoryToken): DeliveryReceipt {
  return { adapter: ADAPTER_NAME, token };
}

export function readToken(receipt: DeliveryReceipt): MemoryToken {
  if (receipt === null || typeof receipt !== "object") {
    throw new TaskrailError("DeliveryReceipt가 아니다");
  }
  if (receipt.adapter !== ADAPTER_NAME) {
    throw new TaskrailError(
      `"${receipt.adapter}" 어댑터의 receipt를 memory 큐에 줄 수 없다`,
    );
  }

  const token = receipt.token as Partial<MemoryToken> | null | undefined;
  if (
    token === null ||
    typeof token !== "object" ||
    typeof token.queue !== "string" ||
    typeof token.messageId !== "number" ||
    typeof token.deliveryId !== "number"
  ) {
    throw new TaskrailError("memory receipt의 token 형식이 올바르지 않다");
  }

  return { queue: token.queue, messageId: token.messageId, deliveryId: token.deliveryId };
}
