/** 헤더 값은 임의 바이트다 — UTF-8 문자열이라는 보장이 없다. */
export interface WireHeader {
  readonly key: string;
  readonly value: Buffer;
}

export type WireRequest =
  | {
      readonly kind: "publish";
      readonly topic: string;
      readonly headers: readonly WireHeader[];
      readonly payload: Buffer;
    }
  | {
      readonly kind: "fetch";
      readonly topic: string;
      readonly consumerId: string;
      readonly maxMessages: number;
    }
  | {
      readonly kind: "ack";
      readonly topic: string;
      readonly consumerId: string;
      readonly offset: bigint;
    }
  | { readonly kind: "topic-create"; readonly topic: string };

export interface FetchedRecord {
  readonly offset: bigint;
  readonly timestamp: bigint;
  readonly headers: readonly WireHeader[];
  readonly payload: Buffer;
}

export type WireResponse =
  | { readonly kind: "publish-ok"; readonly correlationId: number; readonly offset: bigint }
  | {
      readonly kind: "fetch-ok";
      readonly correlationId: number;
      readonly records: readonly FetchedRecord[];
    }
  | { readonly kind: "ack-ok"; readonly correlationId: number }
  | { readonly kind: "topic-create-ok"; readonly correlationId: number }
  | {
      readonly kind: "error";
      readonly correlationId: number;
      readonly code: number;
      readonly message: string;
    };
