import {Kafka, logLevel}  from 'kafkajs'
import delay from 'delay';

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`127.0.0.1:9092`],
  clientId: "example-consumer",
});

const topic = "";
const consumer = kafka.consumer({ groupId: "test-group" });

const run = async () => {
  await consumer.connect();
  const regx = new RegExp('^'+topic+'([A-Za-z]+)$')
  await consumer.subscribe({topics: [regx], fromBeginning: true});
  await consumer.run({
    autoCommit: true,  
    eachMessage: async ({ topic, partition, message }) => {
      console.log('partition <---- ',partition)
      console.log(`<--- [${topic}] \t\t <---  \t\t\t\t\t`,message && message.key ? message.key.toString(): null,' --- ',message && message.value ? message.value.toString(): null)
    },
  });
};

const runBatch = async () => {
  await consumer.connect()
  const regx = new RegExp('^'+topic+'([A-Za-z]+)$')
  await consumer.subscribe({topics: [regx], fromBeginning: true});
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
          seperateAsync = {}
          for (let message of batch.messages) {
              // console.log('is runing --> ',isRunning(),'\n----> is stale - ',isStale())
              if (!isRunning() || isStale()) {
                break
              } else  {
                await convertToModel(batch, message)
              }
          }
          await processAsyncResolveOffset(seperateAsync, resolveOffset, heartbeat)
      },
  })
}

const processAsyncResolveOffset = async(seperateAsync: any, resolveOffset: any, heartbeat: any) => {
  // console.log('seperateAsync||||||',seperateAsync)
  for (const key in seperateAsync) {
    // console.log('key++++++',key)
    userProcess(key, seperateAsync, resolveOffset, heartbeat) // syncronous
  }
}

const userProcess = async (key: any, seperateAsync: any, resolveOffset: any, heartbeat: any) => {
  const taskArr = seperateAsync[key]
  for (const message of taskArr) {
    // console.log('message ++++ ',message)
    await todoSomething(message)
    // await resolveOffset(message.offset)
    // await heartbeat()
  }
}

const todoSomething = async (message: any) => {
  console.log(`<--- [${message.batch.topic}] \t\t <---  \t\t\t\t\t`,message && message.key ? message.key.toString(): null,' --- ',message && message.value ? message.value.toString(): null)
  const steps = [1,2,3,4,5]
  for(const step of steps)
  {
    console.log('\t\t\t\t\t\t\---------- ',message && message.key ? message.key.toString(): null,' --- ',message && message.value ? message.value.toString(): null,'---do process = ',step);
    await delay(2000)
  }
}

let seperateAsync: any = {}
const convertToModel = async (batch: any, message: any) => {
  message.batch = batch
  if(!seperateAsync[message.key]) seperateAsync[message.key] = []
  seperateAsync[message.key].push(message)
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
