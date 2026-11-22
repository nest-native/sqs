import { SqsDriver } from './driver';
import { SqsQueueOptions } from './interfaces';
import { DEFAULT_SQS_CLIENT, SqsClients } from './sqs-clients';
import { SqsQueueInfo } from './sqs-context';

/**
 * A resolved queue, with the driver of the client that talks to it.
 *
 * @internal
 */
export interface ResolvedSqsQueue extends SqsQueueInfo {
  readonly driver: SqsDriver;
}

/**
 * Turns the logical keys handlers and the producer use into queues.
 *
 * A configured key (`queues`) resolves to its name or URL on its client; any
 * other key is taken as a URL when it starts with `http://` or `https://`, and
 * as a queue name on the default client otherwise. A name costs one
 * `GetQueueUrl`, cached: a queue URL is fixed by its account and name, so it
 * does not change even when the queue is deleted and created again. A lookup
 * that failed is not cached.
 *
 * @internal
 */
export class SqsQueueRegistry {
  private readonly urls = new Map<string, Promise<string>>();

  constructor(
    private readonly queues: Record<string, string | SqsQueueOptions>,
    private readonly clients: SqsClients,
  ) {}

  /** The configuration of a logical key, normalized to an object. */
  options(key: string): SqsQueueOptions {
    const configured = this.queues[key];
    if (configured === undefined) {
      return locator(key);
    }
    if (typeof configured === 'string') {
      return locator(configured);
    }
    if (!configured.url && !configured.name) {
      throw new Error(`The queue "${key}" needs a "name" or a "url".`);
    }
    return configured;
  }

  async resolve(key: string): Promise<ResolvedSqsQueue> {
    const options = this.options(key);
    const client = options.client ?? DEFAULT_SQS_CLIENT;
    const driver = this.clients.driver(client);
    const url = options.url ?? (await this.lookup(driver, client, options.name as string, options.ownerAccountId));
    const name = options.url ? queueNameFromUrl(options.url) : (options.name as string);
    return { key, name, url, isFifo: name.endsWith('.fifo'), client, driver };
  }

  private lookup(driver: SqsDriver, client: string, name: string, owner: string | undefined): Promise<string> {
    const cacheKey = `${client}\u0000${owner ?? ''}\u0000${name}`;
    let pending = this.urls.get(cacheKey);
    if (!pending) {
      pending = driver.getQueueUrl(name, owner);
      this.urls.set(cacheKey, pending);
      pending.catch(() => this.urls.delete(cacheKey));
    }
    return pending;
  }
}

function locator(value: string): SqsQueueOptions {
  return /^https?:\/\//i.test(value) ? { url: value } : { name: value };
}

/** The last segment of a queue URL's path: the queue's name. */
function queueNameFromUrl(url: string): string {
  const path = new URL(url).pathname.replace(/\/+$/, '');
  return path.slice(path.lastIndexOf('/') + 1);
}
