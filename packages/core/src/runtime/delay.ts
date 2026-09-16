/** abort되면 false. 남은 시간을 기다렸으면 true. */
export function sleep(ms: number, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) return Promise.resolve(false);
  if (ms <= 0) return Promise.resolve(true);

  return new Promise<boolean>((resolve) => {
    const onAbort = (): void => {
      clearTimeout(timer);
      resolve(false);
    };
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve(true);
    }, ms);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

export type DelayDecision =
  | { readonly kind: "run" }
  | { readonly kind: "sleep"; readonly ms: number }
  | { readonly kind: "requeue"; readonly ms: number };

export const RUN_NOW: DelayDecision = { kind: "run" };

export interface DelayWindow {
  readonly inlineDelayMs: number;
  readonly requeueIntervalMs: number;
}

/**
 * `nativeDelay`가 없는 어댑터 보완용. 짧은 잔여 지연은 슬롯을 잡은 채 기다리고,
 * 긴 지연은 슬롯을 점유하지 않도록 같은 envelope을 다시 큐에 넣는다.
 */
export function decideDelay(
  notBefore: number,
  now: number,
  window: DelayWindow,
): DelayDecision {
  const remaining = notBefore - now;
  if (remaining <= 0) return RUN_NOW;
  if (remaining <= window.inlineDelayMs) return { kind: "sleep", ms: remaining };
  return { kind: "requeue", ms: Math.min(remaining, window.requeueIntervalMs) };
}
