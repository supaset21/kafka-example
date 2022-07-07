const { Kafka, CompressionTypes, logLevel } = require("kafkajs");

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`127.0.0.1:9092`],
  clientId: "example-producer",
});

const topic = "my-topic";
const producer = kafka.producer();

const getRandomNumber = () => Math.round(Math.random(10) * 1000);
const createMessage = (num) => ({
  key: `key-${num}`,
  value: `value-${num}-${new Date().toISOString()}`,
});

const sendMessage = () => {
  const messages = Array(getRandomNumber())
    .fill()
    .map((_) => createMessage(getRandomNumber()));

  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      // messages: [
      //   { key: "key1", value: "hello world", partition: 0 },
      //   { key: "key2", value: "hey hey!", partition: 1 },
      // ],
      messages: messages,
    })
    .then(console.log)
    .catch((e) => console.error(`[example/producer] ${e.message}`, e));
};

const run = async () => {
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
