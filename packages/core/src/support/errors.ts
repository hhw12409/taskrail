/** handler가 "재시도해도 성공할 수 없다"를 선언하는 수단. 재시도 없이 즉시 DEAD. */
export class FatalJobError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options as ErrorOptions | undefined);
    this.name = "FatalJobError";
  }
}

export class TimeoutError extends Error {
  readonly timeoutMs: number;

  constructor(timeoutMs: number, message?: string) {
    super(message ?? `job timed out after ${timeoutMs}ms`);
    this.name = "TimeoutError";
    this.timeoutMs = timeoutMs;
  }
}

/** 설정/사용 오류 (중복 정의, 직렬화 불가 payload, delayMs+runAt 동시 지정 등). */
export class TaskrailError extends Error {
  constructor(message: string, options?: { cause?: unknown }) {
    super(message, options as ErrorOptions | undefined);
    this.name = "TaskrailError";
  }
}

export function isFatalJobError(error: unknown): error is FatalJobError {
  return error instanceof FatalJobError;
}

/** handler는 Error가 아닌 값으로도 reject할 수 있다. */
export function describeError(error: unknown): { message: string; stack?: string } {
  if (error instanceof Error) {
    return error.stack === undefined
      ? { message: error.message }
      : { message: error.message, stack: error.stack };
  }
  try {
    return {
      message: typeof error === "string" ? error : JSON.stringify(error) ?? String(error),
    };
  } catch {
    return { message: String(error) };
  }
}
