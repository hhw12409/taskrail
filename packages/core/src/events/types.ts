import type { DeadLetterReason } from "../queue/queue.js";

export interface TaskrailEvents {
  "job.enqueued": (e: {
    jobId: string;
    name: string;
    queue: string;
    notBefore: number;
    attempt: number;
  }) => void;
  "job.started": (e: {
    jobId: string;
    name: string;
    attempt: number;
    deliveryCount: number;
  }) => void;
  "job.completed": (e: {
    jobId: string;
    name: string;
    attempt: number;
    durationMs: number;
  }) => void;
  "job.failed": (e: {
    jobId: string;
    name: string;
    attempt: number;
    error: unknown;
    timedOut: boolean;
  }) => void;
  "job.retried": (e: {
    jobId: string;
    name: string;
    nextAttempt: number;
    delayMs: number;
  }) => void;
  "job.dead": (e: {
    jobId: string;
    name: string;
    attempt: number;
    reason: DeadLetterReason;
  }) => void;
  "worker.active": (e: { workerId: string; inFlight: number }) => void;
  "worker.idle": (e: { workerId: string }) => void;
}

export type TaskrailEventName = keyof TaskrailEvents;

export type TaskrailEventPayload<E extends TaskrailEventName> = Parameters<
  TaskrailEvents[E]
>[0];
