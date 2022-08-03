import { Kafka, Partitioners, CompressionTypes, logLevel } from "kafkajs";
import { resolve } from "path";

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`127.0.0.1:9092`],
  clientId: "example-producer",
});
 
const producer = kafka.producer();

const getClient = (input: any) => {
  const xxx = ['MGM', 'AMBFUN']
  // console.log(input,'=======>',xxx[input])
  return xxx[input]
}

const topic1 = `${getClient(0)}`;
const topic2 = `${getClient(1)}`;
const numPartitions = 3;  // if you have 4 partition custom then +1 = 5 cause 0 is deault

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

const getRandomNumber = (num: any) => Math.round(Math.random() * num);
const createMessage = (CLIENT: any, userName: string) => {
  // const partition = getPartition(CLIENT, GAME)
  // console.log('partition------>',partition)
  const key = `${CLIENT+"-"+userName}`
  const action = `${descriptState(getStateByKey(key))}`
  if(!action) return undefined
  return {
    key,
    value: action,
    // partition: partition
    // partition
  };
}

const descriptState = (num: any) => {
  const xxx = [' B E T ', 'SETTLE', 'A-C-T-3', 'A-C-T-4', 'A-C-T-5', 'A-C-T-6', 'A-C-T-7', 'A-C-T-8']
  return xxx[num-1]
}

const sendMessage = async () => {
  const CLIENT = `${getClient(getRandomNumber(1))}`
  // const GAME = getGame(getRandomNumber())
  const userName = `user-${getRandomNumber(4)}`
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
  console.log('send messages --->\t', messages[0].key,'\t:\t',messages[0].value)
  const result = await sendMessageRes(topic, messages)
  return result
};

const sendMessageRes =  async (topic:any, messages: any) => {
  return new Promise((resolve, reject)=> {
    producer
    .send({
      topic: topic,
      compression: CompressionTypes.GZIP,
      // messages: [
      //   { key: "key-1", value: "hello world", partition: 0 },
      //   { key: "key-2", value: "hey hey!", partition: 1 },
      // ],
      messages: messages,
    })
    .then((result:any) => { 
      /*if(result && result[0]) console.log(`--> [${result[0].topicName}]\t\t - \t\tpartition[${result[0].partition}] - offset[${result[0].baseOffset}]`)*/
      resolve(result)
    })
    .catch((e) => {
      console.error(`[example/producer] ${e.message}`, e)
      reject(e)
    });
  })
  
  
}

const getPartition = (CLIENT: string, GAME: string) => {
  if(typeof CLIENT == 'string' && CLIENT) CLIENT = CLIENT.toLowerCase()
  if(typeof GAME == 'string' && GAME) GAME = GAME.toLowerCase()
  const xxx: any = { 
    mgmpg: 1, mgmslotxo: 2, mgmambslot: 3, mgmyeekee: 4,
    g2gbetpg: 1, g2gbetslotxo: 2, g2gbetambslot: 3, g2gbetyeekee: 4,
    ambfunpg: 1, ambfunslotxo: 2, ambfunambslot: 3, ambfunyeekee: 4,
  }
  // console.log(input,' ---- ',xxx[input])
  return xxx[`${CLIENT+GAME}`]
}

const getGame = (input: any) => {
  const xxx = ['pg', 'slotxo', 'ambslot', 'yeekee']
  // console.log(`${xxx}.${input} = ${xxx[input]}`)
  return xxx[input]
}

let clientUsers: any = {}
const getStateByKey = (key: string) => {
  if(clientUsers[key]) clientUsers[key] = clientUsers[key]+1
  else clientUsers[key] = 1
  // console.log(users)
  return clientUsers[key]
}


const run = async () => {
  console.log('will send message interval every 3 sec.')
  await createTopic();
  await producer.connect();
  // sendMessage();
  setInterval(sendMessage, 3000);

  console.log("Kafka Connected");
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
