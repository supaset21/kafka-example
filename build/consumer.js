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
    clientId: "example-consumer",
});
const topic = "my-topic-";
const consumer = kafka.consumer({ groupId: "test-group" });
const run = () => __awaiter(void 0, void 0, void 0, function* () {
    yield consumer.connect();
    yield consumer.subscribe({ topic, fromBeginning: true });
    yield consumer.run({
        autoCommit: true,
        // eachBatch: async ({ batch }) => {
        //   console.log(batch)
        // },
        eachMessage: ({ topic, partition, message }) => __awaiter(void 0, void 0, void 0, function* () {
            const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`;
            console.log(`- ${prefix} ${message.key}#${message.value}`);
        }),
    });
});
// ['SPORTBOOK88', 'MGM', 'AMB_SPORTBOOK']
const runBatch = () => __awaiter(void 0, void 0, void 0, function* () {
    yield consumer.connect();
    yield consumer.subscribe({ topic: `${topic}SPORTBOOK88`, fromBeginning: true });
    yield consumer.subscribe({ topic: `${topic}MGM`, fromBeginning: true });
    yield consumer.subscribe({ topic: `${topic}AMB_SPORTBOOK`, fromBeginning: true });
    yield consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, uncommittedOffsets, isRunning, isStale, pause, }) => __awaiter(void 0, void 0, void 0, function* () {
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
                if (!isRunning() || isStale())
                    break;
                yield processMessage(batch.topic, message);
                resolveOffset(message.offset);
                yield heartbeat();
            }
        }),
    });
});
const processMessage = (topic, message) => __awaiter(void 0, void 0, void 0, function* () {
    console.log(`consumer topic[${topic}] message -->  `, message);
    // TO DO process follow topic.
});
// run().catch((e) => console.error(`[example/consumer] ${e.message}`, e));
runBatch().catch((e) => console.error(`[example/consumer run batch] ${e.message}`, e));
const errorTypes = ["unhandledRejection", "uncaughtException"];
const signalTraps = ["SIGTERM", "SIGINT", "SIGUSR2"];
errorTypes.forEach((type) => {
    process.on(type, (e) => __awaiter(void 0, void 0, void 0, function* () {
        try {
            console.log(`process.on ${type}`);
            console.error(e);
            yield consumer.disconnect();
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
            yield consumer.disconnect();
        }
        finally {
            process.kill(process.pid, type);
        }
    }));
});
