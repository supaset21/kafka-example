import {Kafka, logLevel}  from 'kafkajs'

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`127.0.0.1:9092`],
  clientId: "example-consumer",
});

const topic = "my-topic-";
const consumer = kafka.consumer({ groupId: "test-group" });

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  await consumer.run({
    autoCommit: true, // for acknowlage
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix} ${message.key}#${message.value}`);
    },
  });
};

// ['SPORTBOOK88', 'MGM', 'AMB_SPORTBOOK']
const runBatch = async () => {
  await consumer.connect()
  await consumer.subscribe({topic: `${topic}SPORTBOOK88`, fromBeginning: true});
  await consumer.subscribe({topic: `${topic}MGM`, fromBeginning: true});
  await consumer.subscribe({topic: `${topic}AMB_SPORTBOOK`, fromBeginning: true});
  await consumer.run({
      eachBatchAutoResolve: true,
      eachBatch: async ({
          batch,
          resolveOffset,
          heartbeat,
          commitOffsetsIfNecessary,
          uncommittedOffsets,
          isRunning,
          isStale,
          pause,
      }) => {
          for (let message of batch.messages) {
              // console.log({
              //     topic: batch.topic,
              //     partition: batch.partition,
              //     highWatermark: batch.highWatermark,
              //     message: {
              //         offset: message.offset,
              //         key: message && message.key ? message.key.toString(): null,
              //         value: message && message.value ? message.value.toString(): null,
              //         headers: message.headers,
              //     }
              // })
              if (!isRunning() || isStale()) break
              await processMessage(batch.topic, message)
              resolveOffset(message.offset)
              await heartbeat()
          }
      },
  })
}

const processMessage = async (topic: string, message: any) => {
  console.log(`consumer topic[${topic}] message -->  `,message);
  // TO DO process follow topic.
}

// run().catch((e) => console.error(`[example/consumer] ${e.message}`, e));
runBatch().catch((e) => console.error(`[example/consumer run batch] ${e.message}`, e));


const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.forEach((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await consumer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});
