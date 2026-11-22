#!/usr/bin/env node
/**
 * Wait until an SQS-compatible endpoint answers ListQueues, through the AWS SDK
 * (so the AWS JSON protocol is what gets checked). Used by `infra:up` and by
 * CI, because the ElasticMQ native image is distroless and cannot run a health
 * check of its own.
 *
 *   node scripts/wait-for-sqs.mjs http://localhost:9324 [timeoutMs]
 */
import { ListQueuesCommand, SQSClient } from '@aws-sdk/client-sqs';

const endpoint = process.argv[2] ?? process.env.SQS_ENDPOINT;
const timeoutMs = Number(process.argv[3] ?? 60_000);
if (!endpoint) {
  console.error('Usage: node scripts/wait-for-sqs.mjs <endpoint> [timeoutMs]');
  process.exit(2);
}

const client = new SQSClient({
  endpoint,
  region: process.env.AWS_REGION ?? 'us-east-1',
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID ?? 'test',
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY ?? 'test',
  },
  maxAttempts: 1,
});

const deadline = Date.now() + timeoutMs;
let attempt = 0;
for (;;) {
  attempt += 1;
  try {
    await client.send(new ListQueuesCommand({}), { abortSignal: AbortSignal.timeout(2_000) });
    console.log(`${endpoint} answered after ${attempt} attempt(s).`);
    client.destroy();
    process.exit(0);
  } catch (error) {
    if (Date.now() > deadline) {
      console.error(`${endpoint} did not answer within ${timeoutMs} ms: ${error?.message ?? error}`);
      client.destroy();
      process.exit(1);
    }
    await new Promise(resolve => setTimeout(resolve, 1_000));
  }
}
