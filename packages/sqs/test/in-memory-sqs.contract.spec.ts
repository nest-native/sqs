import { InMemorySqs, InMemorySqsQueueAttributes } from '../testing/in-memory-sqs';
import { VirtualClock } from '../testing/virtual-clock';
import { sqsSemanticsContract } from './contract/sqs-semantics.contract';

sqsSemanticsContract('InMemorySqs', async () => {
  const clock = new VirtualClock();
  const sqs = new InMemorySqs({ clock });
  return {
    driver: sqs.createDriver(),
    createQueue: async (name, attributes) => sqs.createQueue(name, attributes as InMemorySqsQueueAttributes),
    wait: ms => clock.advance(ms),
  };
});
