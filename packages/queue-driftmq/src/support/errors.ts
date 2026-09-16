import { TaskrailError } from "@taskrail/core";

import { ErrorCode, errorCodeName } from "../wire/protocol.js";

export class DriftmqError extends TaskrailError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "DriftmqError";
  }
}

/** 프레임이 구조적으로 깨졌다. 스트림 동기화가 깨진 것이므로 소켓을 살려 두지 않는다. */
export class ProtocolError extends DriftmqError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ProtocolError";
  }
}

/** 소켓 수준 실패(연결 불가, 끊김, 응답 없음). 재연결로 회복 가능하다. */
export class ConnectionError extends DriftmqError {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options);
    this.name = "ConnectionError";
  }
}

/** 브로커가 ERROR 프레임으로 돌려준 실패. */
export class BrokerError extends DriftmqError {
  readonly code: number;
  readonly codeName: string;

  constructor(code: number, message: string) {
    super(`driftmq ${errorCodeName(code)}: ${message}`);
    this.name = "BrokerError";
    this.code = code;
    this.codeName = errorCodeName(code);
  }

  /** MALFORMED_FRAME은 브로커가 ERROR를 쓴 직후 연결을 끊는다. 다른 코드는 연결이 살아 있다. */
  get fatal(): boolean {
    return this.code === ErrorCode.MALFORMED_FRAME;
  }
}

export function isBrokerError(error: unknown, code?: number): error is BrokerError {
  if (!(error instanceof BrokerError)) return false;
  return code === undefined || error.code === code;
}
