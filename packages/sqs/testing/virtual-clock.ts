import { SqsClock } from '../clock';

interface VirtualTimer {
  at: number;
  order: number;
  callback: () => void;
}

/**
 * A clock that only moves when told to. `InMemorySqs` uses one by default and
 * `SqsTestModule` hands it to the module, so visibility timeouts, receive
 * deadlines and backoffs all follow {@link advance}: a test that needs a
 * message's visibility to expire moves time instead of sleeping.
 *
 * @publicApi
 */
export class VirtualClock implements SqsClock {
  private current: number;
  private sequence = 0;
  private readonly timers = new Map<number, VirtualTimer>();

  constructor(start = Date.UTC(2026, 0, 1)) {
    this.current = start;
  }

  now(): number {
    return this.current;
  }

  setTimeout(callback: () => void, ms: number): number {
    this.sequence += 1;
    this.timers.set(this.sequence, { at: this.current + Math.max(0, ms), order: this.sequence, callback });
    return this.sequence;
  }

  clearTimeout(timer: unknown): void {
    this.timers.delete(timer as number);
  }

  /** How many timers are waiting. */
  get pendingTimers(): number {
    return this.timers.size;
  }

  /**
   * Move time forward by `ms`, firing every timer that falls due, in order,
   * and letting the promise work each one triggers run before the next.
   */
  async advance(ms: number): Promise<void> {
    const target = this.current + ms;
    await settle();
    for (let next = this.nextDue(target); next; next = this.nextDue(target)) {
      this.timers.delete(next.order);
      this.current = next.at;
      next.callback();
      await settle();
    }
    this.current = target;
    await settle();
  }

  private nextDue(target: number): VirtualTimer | undefined {
    let next: VirtualTimer | undefined;
    for (const timer of this.timers.values()) {
      // Timers iterate in creation order, so the first of equal deadlines wins.
      if (timer.at <= target && (!next || timer.at < next.at)) {
        next = timer;
      }
    }
    return next;
  }
}

/** Let pending promise chains, and `setImmediate` work, run. */
async function settle(): Promise<void> {
  for (let round = 0; round < 10; round += 1) {
    await new Promise<void>(resolve => setImmediate(resolve));
  }
}
