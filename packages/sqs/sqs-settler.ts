import { LoggerService } from '@nestjs/common';
import { SQS_MAX_BATCH_ENTRIES } from './constants';
import { SqsBatchOutcome, SqsDriver } from './driver';

interface PendingSettle {
  receiptHandle: string;
  visibilityTimeout?: number;
  resolve(settled: boolean): void;
}

type SettleOperation = 'delete' | 'visibility change';

/**
 * Settles the messages of one queue: deletes them, or changes their
 * visibility. Requests made in the same macrotask are coalesced into one
 * batch call (up to 10 entries), and every per-entry result is checked,
 * because a batch call returns HTTP 200 even when some of its entries failed.
 *
 * A settle that fails is logged and reported as `false`, never retried by
 * making the message visible again: the SDK has already retried transient
 * errors, and a message that was processed must not be re-shown on purpose.
 *
 * @internal
 */
export class SqsSettler {
  private readonly deletes: PendingSettle[] = [];
  private readonly changes: PendingSettle[] = [];
  private readonly running = new Set<Promise<void>>();
  private scheduled = false;

  constructor(
    private readonly driver: SqsDriver,
    private readonly queueUrl: string,
    private readonly queueKey: string,
    private readonly logger: LoggerService,
  ) {}

  /** Delete a message. Resolves `true` once SQS confirmed it. */
  delete(receiptHandle: string): Promise<boolean> {
    return this.enqueue(this.deletes, { receiptHandle });
  }

  /** Change a message's visibility. Resolves `true` once SQS confirmed it. */
  changeVisibility(receiptHandle: string, visibilityTimeout: number): Promise<boolean> {
    return this.enqueue(this.changes, { receiptHandle, visibilityTimeout });
  }

  /** Send everything pending now, and wait for every call in flight. */
  async flush(): Promise<void> {
    this.dispatchPending();
    await Promise.all([...this.running]);
  }

  private enqueue(queue: PendingSettle[], entry: Omit<PendingSettle, 'resolve'>): Promise<boolean> {
    return new Promise(resolve => {
      queue.push({ ...entry, resolve });
      if (queue.length >= SQS_MAX_BATCH_ENTRIES) {
        this.dispatchPending();
      } else {
        this.schedule();
      }
    });
  }

  private schedule(): void {
    if (this.scheduled) {
      return;
    }
    this.scheduled = true;
    setImmediate(() => {
      this.scheduled = false;
      this.dispatchPending();
    });
  }

  private dispatchPending(): void {
    while (this.deletes.length > 0) {
      const batch = this.deletes.splice(0, SQS_MAX_BATCH_ENTRIES);
      this.track(
        this.settle('delete', batch, () =>
          this.driver.deleteMessages(
            this.queueUrl,
            batch.map((entry, index) => ({ id: String(index), receiptHandle: entry.receiptHandle })),
          ),
        ),
      );
    }
    while (this.changes.length > 0) {
      const batch = this.changes.splice(0, SQS_MAX_BATCH_ENTRIES);
      this.track(
        this.settle('visibility change', batch, () =>
          this.driver.changeVisibility(
            this.queueUrl,
            batch.map((entry, index) => ({
              id: String(index),
              receiptHandle: entry.receiptHandle,
              visibilityTimeout: entry.visibilityTimeout as number,
            })),
          ),
        ),
      );
    }
  }

  private track(work: Promise<void>): void {
    this.running.add(work);
    void work.finally(() => this.running.delete(work));
  }

  private async settle(
    operation: SettleOperation,
    batch: PendingSettle[],
    call: () => Promise<SqsBatchOutcome>,
  ): Promise<void> {
    let failedIds: Set<string>;
    try {
      const outcome = await call();
      failedIds = new Set(outcome.failed.map(failure => failure.id));
      for (const failure of outcome.failed) {
        this.logger.warn(
          `SQS refused a ${operation} on "${this.queueKey}": ${failure.code}` +
            `${failure.message ? ` (${failure.message})` : ''}.`,
        );
      }
    } catch (error) {
      failedIds = new Set(batch.map((_, index) => String(index)));
      this.logger.warn(
        `A ${operation} of ${batch.length} message(s) on "${this.queueKey}" failed: ${describe(error)}.`,
      );
    }
    batch.forEach((entry, index) => entry.resolve(!failedIds.has(String(index))));
  }
}

function describe(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
