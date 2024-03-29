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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const kafkajs_1 = require("kafkajs");
const delay_1 = __importDefault(require("delay"));
const utils_1 = __importDefault(require("./utils"));
const kafka = new kafkajs_1.Kafka({
    logLevel: kafkajs_1.logLevel.INFO,
    brokers: [`127.0.0.1:9092`],
    clientId: "example-consumer",
});
const topic = utils_1.default.getTopic();
const consumer = kafka.consumer({ groupId: "test-group" });
const run = () => __awaiter(void 0, void 0, void 0, function* () {
    yield consumer.connect();
    const regx = utils_1.default.allTopic(topic);
    yield consumer.subscribe({ topics: [regx], fromBeginning: true });
    yield consumer.run({
        autoCommit: true,
        eachMessage: ({ topic, partition, message }) => __awaiter(void 0, void 0, void 0, function* () {
            console.log('partition <---- ', partition);
            console.log(`<--- [${topic}] \t\t <---  \t\t\t\t\t`, message && message.key ? message.key.toString() : null, ' --- ', message && message.value ? message.value.toString() : null);
        }),
    });
});
const runBatch = () => __awaiter(void 0, void 0, void 0, function* () {
    yield consumer.connect();
    const regx = utils_1.default.allTopic(topic);
    yield consumer.subscribe({ topics: [regx], fromBeginning: true });
    yield consumer.run({
        eachBatchAutoResolve: true,
        eachBatch: ({ batch, resolveOffset, heartbeat, commitOffsetsIfNecessary, uncommittedOffsets, isRunning, isStale, pause, }) => __awaiter(void 0, void 0, void 0, function* () {
            seperateAsync = {};
            console.log('batch [', batch.messages.length, ']');
            for (let message of batch.messages) {
                // console.log('is runing --> ',isRunning(),'\n----> is stale - ',isStale())
                if (!isRunning() || isStale()) {
                    break;
                }
                else {
                    console.log('--convert--[', message && message.key ? message.key.toString() : null, ' --- ', message && message.value ? message.value.toString() : null, ']');
                    yield convertToModel(batch, message);
                    console.log('---- end convert ----');
                }
            }
            console.log('start process resoleve off set.');
            yield processAsyncResolveOffset(seperateAsync, resolveOffset, heartbeat);
            console.log('end resolve offset.\n------------\n');
        }),
    });
});
const processAsyncResolveOffset = (seperateAsync, resolveOffset, heartbeat) => __awaiter(void 0, void 0, void 0, function* () {
    // console.log('seperateAsync||||||',seperateAsync)
    const usersExecute = [];
    for (const key in seperateAsync) {
        // console.log('key++++++',key)
        usersExecute.push(yield userProcess(key, seperateAsync, resolveOffset, heartbeat)); // syncronous
    }
    yield Promise.all([usersExecute]);
});
const userProcess = (key, seperateAsync, resolveOffset, heartbeat) => __awaiter(void 0, void 0, void 0, function* () {
    const taskArr = seperateAsync[key];
    for (const message of taskArr) {
        // console.log('message ++++ ',message)
        yield todoSomething(message);
        yield resolveOffset(message.offset);
        yield heartbeat();
    }
});
const todoSomething = (message) => __awaiter(void 0, void 0, void 0, function* () {
    console.log(`\t\t\t\t\t\t <---  \t\t\t\t\t`, message && message.key ? message.key.toString() : null, ' --- ', message && message.value ? message.value.toString() : null);
    const steps = [1, 2, 3, 4, 5];
    for (const step of steps) {
        console.log('\t\t\t\t\t\t\---------- ', message && message.key ? message.key.toString() : null, ' --- ', message && message.value ? message.value.toString() : null, '---do process = ', step);
        yield delay_1.default(utils_1.default.getChoiceDelay(utils_1.default.getRandomNumber(2)));
    }
});
let seperateAsync = {};
const convertToModel = (batch, message) => __awaiter(void 0, void 0, void 0, function* () {
    message.batch = batch;
    if (!seperateAsync[message.key])
        seperateAsync[message.key] = [];
    yield seperateAsync[message.key].push(message);
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
