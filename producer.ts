import { Kafka, Partitioners, CompressionTypes, logLevel } from "kafkajs";
import Utils from './utils'
import delay from 'delay';

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`127.0.0.1:9092`],
  clientId: "example-producer",
});
 
const producer = kafka.producer();

const topic = Utils.getTopic();
const topic1 = `${topic}${Utils.getClient(0)}`;
const topic2 = `${topic}${Utils.getClient(1)}`;
const numPartitions = 300;  // if you have 4 partition custom then +1 = 5 cause 0 is deault

const createTopic = async () => {
  const admin = kafka.admin();
  await admin.connect();
  await admin.createTopics({
    topics: [
      {topic:topic1,numPartitions},
      {topic:topic2,numPartitions}
    ],
  });
  await admin.disconnect();
}

const createMessage = (CLIENT: any, userName: string) => {
  // const partition = getPartition(CLIENT, GAME)
  // console.log('partition------>',partition)
  const key = `${CLIENT+"-"+userName}`
  const action = `${Utils.descriptState(getStateByKey(key))}`
  if(!action) return undefined
  return {
    key,
    value: action,
    // partition: partition
    // partition
  };
}

const sendMessage = async () => {
  const CLIENT = `${Utils.getClient(Utils.getRandomNumber(0))}`
  // const GAME = getGame(getRandomNumber())
  const userName = `user-${Utils.getRandomNumber(1)}`
  const topic = `${CLIENT}`
  const messages = [2]
    // .fill() 
    .map((_) => {
      const result = createMessage(CLIENT, userName)
      return result??undefined
    });
  if(!messages[0]) return 0
  if(messages[0]&& messages[0].value == 'undefined') {
    console.log('fuck off')
    process.exit(1)
  }
  // console.log('send messages --->\t', messages[0].key,'\t:\t',messages[0].value)
  const result = await Utils.sendMessageRes(producer, CompressionTypes.GZIP, topic, messages)
  return result
};

let clientUsers: any = {}
const getStateByKey = (key: string) => {
  if(clientUsers[key]) clientUsers[key] = clientUsers[key]+1
  else clientUsers[key] = 1
  // console.log(users)
  return clientUsers[key]
}

const run = async () => {
  await createTopic();
  await producer.connect();
  console.log("Kafka Connected");
  while (true) {
    sendMessage()
    const milliSec = Utils.getChoiceDelay(Utils.getRandomNumber(2))
    console.log(`next message later ${milliSec/1000} sec.`)
    await delay(milliSec)
  }
};

run();

/* For Handle Event Error */
const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];

errorTypes.forEach((type) => {
  process.on(type, async () => {
    try {
      console.log(`process.on ${type}`);
      await producer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.forEach((type) => {
  process.once(type, async () => {
    try {
      console.log("Bye", type);
      await producer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});
/* For Handle Event Error */
