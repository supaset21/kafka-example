"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const kafka = new kafkajs_1.Kafka({
    logLevel: kafkajs_1.logLevel.INFO,
    brokers: [`127.0.0.1:9092`],
    clientId: "example-producer",
});
const topic = "my-topic-";
const producer = kafka.producer();
const getRandomNumber = () => Math.round(Math.random() * 1000);
const createMessage = (num) => ({
    key: `key-${num}`,
    value: `value-${num}-${new Date().toISOString()}`,
});
const sendMessage = () => {
    const messages = [getRandomNumber()]
        // .fill() 
        .map((_) => createMessage(getRandomNumber()));
    // console.log('send messages --->',JSON.stringify(messages))
    return producer
        .send({
        topic: `${topic}${getTopic(Math.floor(Math.random() * 2))}`,
        compression: kafkajs_1.CompressionTypes.GZIP,
        // messages: [
        //   { key: "key1", value: "hello world", partition: 0 },
        //   { key: "key2", value: "hey hey!", partition: 1 },
        // ],
        messages: messages,
    })
        .then(console.log)
        .catch((e) => console.error(`[example/producer] ${e.message}`, e));
};
const getTopic = (num) => {
    const xxx = ['SPORTBOOK88', 'MGM', 'AMB_SPORTBOOK'];
    return xxx[num];
};
const run = () => __awaiter(void 0, void 0, void 0, function* () {
    console.log('will send message interval every 3 sec.');
    yield producer.connect();
    // sendMessage();
    setInterval(sendMessage, 3000);
    console.log("Kafka Connected");
});
run();
/* For Handle Event Error */
const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];
errorTypes.forEach((type) => {
    process.on(type, () => __awaiter(void 0, void 0, void 0, function* () {
        try {
            console.log(`process.on ${type}`);
            yield producer.disconnect();
            process.exit(0);
        }
        catch (_) {
            process.exit(1);
        }
    }));
});
signalTraps.forEach((type) => {
    process.once(type, () => __awaiter(void 0, void 0, void 0, function* () {
        try {
            console.log("Bye", type);
            yield producer.disconnect();
        }
        finally {
            process.kill(process.pid, type);
        }
    }));
});
/* For Handle Event Error */
