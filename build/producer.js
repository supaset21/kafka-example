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
const producer = kafka.producer();
const getClient = (input) => {
    const xxx = ['MGM', 'AMBFUN'];
    // console.log(input,'=======>',xxx[input])
    return xxx[input];
};
const topic1 = `${getClient(0)}`;
const topic2 = `${getClient(1)}`;
const numPartitions = 3; // if you have 4 partition custom then +1 = 5 cause 0 is deault
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
const getRandomNumber = (num) => Math.round(Math.random() * num);
const createMessage = (CLIENT, userName) => {
    // const partition = getPartition(CLIENT, GAME)
    // console.log('partition------>',partition)
    const key = `${CLIENT + "-" + userName}`;
    const action = `${descriptState(getStateByKey(key))}`;
    if (!action)
        return undefined;
    return {
        key,
        value: action,
    };
};
const descriptState = (num) => {
    const xxx = [' B E T ', 'SETTLE', 'A-C-T-3', 'A-C-T-4', 'A-C-T-5', 'A-C-T-6', 'A-C-T-7', 'A-C-T-8'];
    return xxx[num - 1];
};
const sendMessage = () => __awaiter(void 0, void 0, void 0, function* () {
    const CLIENT = `${getClient(getRandomNumber(1))}`;
    // const GAME = getGame(getRandomNumber())
    const userName = `user-${getRandomNumber(4)}`;
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
    console.log('send messages --->\t', messages[0].key, '\t:\t', messages[0].value);
    const result = yield sendMessageRes(topic, messages);
    return result;
});
const sendMessageRes = (topic, messages) => __awaiter(void 0, void 0, void 0, function* () {
    return new Promise((resolve, reject) => {
        producer
            .send({
            topic: topic,
            compression: kafkajs_1.CompressionTypes.GZIP,
            // messages: [
            //   { key: "key-1", value: "hello world", partition: 0 },
            //   { key: "key-2", value: "hey hey!", partition: 1 },
            // ],
            messages: messages,
        })
            .then((result) => {
            /*if(result && result[0]) console.log(`--> [${result[0].topicName}]\t\t - \t\tpartition[${result[0].partition}] - offset[${result[0].baseOffset}]`)*/
            resolve(result);
        })
            .catch((e) => {
            console.error(`[example/producer] ${e.message}`, e);
            reject(e);
        });
    });
});
const getPartition = (CLIENT, GAME) => {
    if (typeof CLIENT == 'string' && CLIENT)
        CLIENT = CLIENT.toLowerCase();
    if (typeof GAME == 'string' && GAME)
        GAME = GAME.toLowerCase();
    const xxx = {
        mgmpg: 1, mgmslotxo: 2, mgmambslot: 3, mgmyeekee: 4,
        g2gbetpg: 1, g2gbetslotxo: 2, g2gbetambslot: 3, g2gbetyeekee: 4,
        ambfunpg: 1, ambfunslotxo: 2, ambfunambslot: 3, ambfunyeekee: 4,
    };
    // console.log(input,' ---- ',xxx[input])
    return xxx[`${CLIENT + GAME}`];
};
const getGame = (input) => {
    const xxx = ['pg', 'slotxo', 'ambslot', 'yeekee'];
    // console.log(`${xxx}.${input} = ${xxx[input]}`)
    return xxx[input];
};
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
    console.log('will send message interval every 3 sec.');
    yield createTopic();
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
