/**
 * `FAILED`는 저장 상태로 남지 않는다 — 같은 tick에 RETRY_WAIT 또는 DEAD로 간다.
 * `job.failed` 이벤트의 관측 지점으로만 존재한다.
 */
export type JobStatus =
  | "WAITING"
  | "RUNNING"
  | "SUCCESS"
  | "FAILED"
  | "RETRY_WAIT"
  | "DEAD";

export const JOB_STATUSES: readonly JobStatus[] = [
  "WAITING",
  "RUNNING",
  "SUCCESS",
  "FAILED",
  "RETRY_WAIT",
  "DEAD",
] as const;

export const TERMINAL_STATUSES: readonly JobStatus[] = ["SUCCESS", "DEAD"] as const;

export function isTerminal(status: JobStatus): boolean {
  return TERMINAL_STATUSES.includes(status);
}

/** RUNNING → WAITING은 lease 만료 재전달/shutdown nack이며 attempt를 소모하지 않는다. */
const ALLOWED_TRANSITIONS: Readonly<Record<JobStatus, readonly JobStatus[]>> = {
  WAITING: ["RUNNING", "DEAD"],
  RUNNING: ["SUCCESS", "FAILED", "WAITING"],
  FAILED: ["RETRY_WAIT", "DEAD"],
  RETRY_WAIT: ["WAITING"],
  SUCCESS: [],
  DEAD: [],
};

export function canTransition(from: JobStatus, to: JobStatus): boolean {
  return ALLOWED_TRANSITIONS[from].includes(to);
}
