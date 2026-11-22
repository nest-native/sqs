import { OnApplicationShutdown } from '@nestjs/common';
import { createAwsSqsDriver, SqsDriver } from './driver';
import { SqsModuleOptions } from './interfaces';

/** The name of the client configured with `client`. */
export const DEFAULT_SQS_CLIENT = 'default';

/**
 * The configured clients: the default one (`client`) and the named ones
 * (`clients`), each behind its driver.
 *
 * Clients the module created from a configuration are destroyed when the
 * application shuts down — after the consumers have drained, so the producer
 * stays usable while they do. Clients you supplied are left alone.
 *
 * @publicApi
 */
export class SqsClients implements OnApplicationShutdown {
  private readonly drivers = new Map<string, SqsDriver>();

  constructor(options: SqsModuleOptions) {
    const factory = options.driverFactory ?? createAwsSqsDriver;
    this.drivers.set(DEFAULT_SQS_CLIENT, factory(options.client ?? {}, DEFAULT_SQS_CLIENT));
    for (const [name, input] of Object.entries(options.clients ?? {})) {
      if (name === DEFAULT_SQS_CLIENT) {
        throw new Error('"clients.default" is reserved: configure the default client with "client".');
      }
      this.drivers.set(name, factory(input, name));
    }
  }

  /** The names of every configured client. */
  names(): string[] {
    return [...this.drivers.keys()];
  }

  /** The underlying client — an `SQSClient`, unless a driver factory says otherwise. */
  get(name: string = DEFAULT_SQS_CLIENT): unknown {
    return this.driver(name).client;
  }

  /** @internal */
  driver(name: string = DEFAULT_SQS_CLIENT): SqsDriver {
    const driver = this.drivers.get(name);
    if (!driver) {
      throw new Error(
        `No SQS client named "${name}" is configured (configured: ${this.names().join(', ')}).`,
      );
    }
    return driver;
  }

  onApplicationShutdown(): void {
    for (const driver of this.drivers.values()) {
      driver.destroy();
    }
  }
}
