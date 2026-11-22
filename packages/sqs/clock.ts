/**
 * The time source the transport schedules against: receive deadlines, the
 * receive-error backoff, the drain timeout and the 12-hour visibility clamp.
 *
 * Production uses {@link systemClock}. `SqsTestModule` swaps in the virtual
 * clock of `InMemorySqs`, so a test moves time instead of sleeping and every
 * timer the transport owns follows it.
 */
export interface SqsClock {
  now(): number;
  setTimeout(callback: () => void, ms: number): unknown;
  clearTimeout(timer: unknown): void;
}

/**
 * Wall-clock time and Node's own timers.
 *
 * The timers are deliberately not `unref()`ed: a consumer backing off after a
 * failed receive may have nothing else keeping the event loop alive, and an
 * application must not exit in the middle of a backoff.
 */
export const systemClock: SqsClock = {
  now: () => Date.now(),
  setTimeout: (callback, ms) => setTimeout(callback, ms),
  clearTimeout: timer => clearTimeout(timer as ReturnType<typeof setTimeout>),
};

/**
 * Wait `ms` on `clock`, or less if `signal` aborts first. Never rejects.
 *
 * @internal
 */
export function sleep(clock: SqsClock, ms: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.resolve();
  }
  return new Promise(resolve => {
    const done = (): void => {
      clock.clearTimeout(timer);
      signal.removeEventListener('abort', done);
      resolve();
    };
    const timer = clock.setTimeout(done, ms);
    signal.addEventListener('abort', done, { once: true });
  });
}

/**
 * Whether `work` settles within `ms` on `clock`. Resolves `true` when it did,
 * `false` when the time ran out first; it never rejects, whatever `work` does.
 *
 * @internal
 */
export function settlesWithin(clock: SqsClock, work: Promise<unknown>, ms: number): Promise<boolean> {
  return new Promise(resolve => {
    const timer = clock.setTimeout(() => resolve(false), ms);
    const settled = (): void => {
      clock.clearTimeout(timer);
      resolve(true);
    };
    work.then(settled, settled);
  });
}
