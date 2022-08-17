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
const utils_1 = __importDefault(require("./utils"));
const delay_1 = __importDefault(require("delay"));
const kafka = new kafkajs_1.Kafka({
    logLevel: kafkajs_1.logLevel.INFO,
    brokers: [`127.0.0.1:9092`],
    clientId: "example-producer",
});
const producer = kafka.producer();
const topic = utils_1.default.getTopic();
const topic1 = `${topic}${utils_1.default.getClient(0)}`;
const topic2 = `${topic}${utils_1.default.getClient(1)}`;
const numPartitions = 300; // if you have 4 partition custom then +1 = 5 cause 0 is deault
const createTopic = () => __awaiter(void 0, void 0, void 0, function* () {
    const admin = kafka.admin();
    yield admin.connect();
    yield admin.createTopics({
        topics: [
            { topic: topic1, numPartitions },
            { topic: topic2, numPartitions }
        ],
    });
    yield admin.disconnect();
});
const createMessage = (CLIENT, userName) => {
    // const partition = getPartition(CLIENT, GAME)
    // console.log('partition------>',partition)
    const key = `${CLIENT + "-" + userName}`;
    const action = `${utils_1.default.descriptState(getStateByKey(key))}`;
    if (!action)
        return undefined;
    return {
        key,
        value: action,
    };
};
const sendMessage = () => __awaiter(void 0, void 0, void 0, function* () {
    const CLIENT = `${utils_1.default.getClient(utils_1.default.getRandomNumber(0))}`;
    // const GAME = getGame(getRandomNumber())
    const userName = `user-${utils_1.default.getRandomNumber(1)}`;
    const topic = `${CLIENT}`;
    const messages = [2]
        // .fill() 
        .map((_) => {
        const result = createMessage(CLIENT, userName);
        return result !== null && result !== void 0 ? result : undefined;
    });
    if (!messages[0])
        return 0;
    if (messages[0] && messages[0].value == 'undefined') {
        console.log('fuck off');
        process.exit(1);
    }
    // console.log('send messages --->\t', messages[0].key,'\t:\t',messages[0].value)
    const result = yield utils_1.default.sendMessageRes(producer, kafkajs_1.CompressionTypes.GZIP, topic, messages);
    return result;
});
let clientUsers = {};
const getStateByKey = (key) => {
    if (clientUsers[key])
        clientUsers[key] = clientUsers[key] + 1;
    else
        clientUsers[key] = 1;
    // console.log(users)
    return clientUsers[key];
};
const run = () => __awaiter(void 0, void 0, void 0, function* () {
    yield createTopic();
    yield producer.connect();
    console.log("Kafka Connected");
    while (true) {
        sendMessage();
        const milliSec = utils_1.default.getChoiceDelay(utils_1.default.getRandomNumber(2));
        console.log(`next message later ${milliSec / 1000} sec.`);
        yield delay_1.default(milliSec);
    }
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
