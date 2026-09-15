import { randomFillSync } from "node:crypto";

const ENCODING = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"; // Crockford Base32
const TIME_LEN = 10;
const RANDOM_LEN = 16;

/** 단조성(monotonic)은 보장하지 않는다 — jobId는 정렬 키가 아니라 식별자다. */
export function ulid(now: number = Date.now()): string {
  return encodeTime(now) + encodeRandom();
}

function encodeTime(now: number): string {
  let time = Math.floor(now);
  let out = "";
  for (let i = 0; i < TIME_LEN; i++) {
    out = ENCODING[time % 32]! + out;
    time = Math.floor(time / 32);
  }
  return out;
}

function encodeRandom(): string {
  const bytes = randomFillSync(new Uint8Array(RANDOM_LEN));
  let out = "";
  for (let i = 0; i < RANDOM_LEN; i++) {
    out += ENCODING[bytes[i]! % 32]!;
  }
  return out;
}
