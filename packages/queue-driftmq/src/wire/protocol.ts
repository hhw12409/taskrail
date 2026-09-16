/** DriftMQ wire protocol v1 상수. 값은 브로커/Java 클라이언트가 공유하는 Codec과 1:1이다. */

export const PROTOCOL_VERSION = 1;

/** 프레임 = [bodyLen: u32 BE][body]. prefix는 body 길이만 센다. */
export const LENGTH_PREFIX_BYTES = 4;

/** body 공통 머리: version(1) + type(1) + correlationId(4). */
export const COMMON_HEADER_BYTES = 6;

/** 초과하면 브로커가 MALFORMED_FRAME을 돌려주고 연결을 끊는다. */
export const MAX_FRAME_BYTES = 16 * 1024 * 1024;

/** FETCH maxMessages가 0 이하일 때 브로커가 적용하는 값. */
export const DEFAULT_FETCH_MAX = 100;

/** 브로커의 (topic, consumerId)당 미ACK 상한. FETCH는 이 값으로 잘린다. */
export const MAX_FETCH_MAX = 1000;

export const U16_MAX = 0xffff;

export const MessageType = {
  PUBLISH: 1,
  FETCH: 2,
  ACK: 3,
  TOPIC_CREATE: 4,
  TOPIC_LIST: 5,
  TOPIC_DESCRIBE: 6,
  PUBLISH_OK: 128,
  FETCH_OK: 129,
  ACK_OK: 130,
  TOPIC_CREATE_OK: 131,
  TOPIC_LIST_OK: 132,
  TOPIC_DESCRIBE_OK: 133,
  ERROR: 255,
} as const;

export type MessageTypeValue = (typeof MessageType)[keyof typeof MessageType];

export const ErrorCode = {
  UNKNOWN_TOPIC: 1,
  TOPIC_ALREADY_EXISTS: 2,
  MALFORMED_FRAME: 3,
  STORAGE_ERROR: 4,
  UNKNOWN_REQUEST_TYPE: 5,
  INTERNAL: 6,
} as const;

export type ErrorCodeValue = (typeof ErrorCode)[keyof typeof ErrorCode];

const ERROR_CODE_NAMES = new Map<number, string>(
  Object.entries(ErrorCode).map(([name, code]) => [code, name]),
);

export function errorCodeName(code: number): string {
  return ERROR_CODE_NAMES.get(code) ?? `UNKNOWN(${code})`;
}
