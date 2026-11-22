/**
 * The capacity of one consumer: one permit per message it holds, from receive
 * to settle. The poller takes permits before it receives and asks SQS for no
 * more messages than it holds permits for, so a received message always has a
 * slot to run in.
 *
 * @internal
 */
export class SqsPermits {
  private free: number;
  private readonly waiters = new Set<() => void>();

  constructor(readonly total: number) {
    this.free = total;
  }

  get available(): number {
    return this.free;
  }

  take(count: number): void {
    this.free -= count;
  }

  give(count: number): void {
    this.free += count;
    if (this.free > 0) {
      for (const wake of [...this.waiters]) {
        wake();
      }
    }
  }

  /**
   * Resolves `true` once at least one permit is free, or `false` as soon as
   * `signal` aborts.
   */
  waitForAvailable(signal: AbortSignal): Promise<boolean> {
    if (signal.aborted) {
      return Promise.resolve(false);
    }
    if (this.free > 0) {
      return Promise.resolve(true);
    }
    return new Promise(resolve => {
      const finish = (available: boolean): void => {
        this.waiters.delete(wake);
        signal.removeEventListener('abort', abort);
        resolve(available);
      };
      const wake = (): void => finish(true);
      const abort = (): void => finish(false);
      this.waiters.add(wake);
      signal.addEventListener('abort', abort, { once: true });
    });
  }
}
