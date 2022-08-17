export default class Utils {
  static getRandomNumber = (num: any) => Math.round(Math.random() * num);
  static getChoiceDelay = (num: any) => {
    const xxx :any = [333,666,1000]
    return xxx[num]
  }
  static allTopic = (topic:string) => new RegExp('^'+topic+'([A-Za-z]+)$')
  static getTopic = ()=> ("")
  static getClient = (input: any) => {
    const xxx = ['MGM', 'AMBFUN']
    return xxx[input]
  }
  static descriptState = (num: any) => {
    const xxx = [' B E T ', 'SETTLE', 'A-C-T-3', 'A-C-T-4', 'A-C-T-5', 'A-C-T-6', 'A-C-T-7', 'A-C-T-8']
    return xxx[num-1]
  }
  static sendMessageRes =  async (producer:any, COMPRESSION: any, topic:any, messages: any) => {
    return new Promise((resolve, reject)=> {
      producer
      .send({
        topic: topic,
        compression: COMPRESSION,
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
      .catch((e: any) => {
        console.error(`[example/producer] ${e.message}`, e)
        reject(e)
      });
    })
  }
  static getPartition = (CLIENT: string, GAME: string) => {
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
  static getGame = (input: any) => {
    const xxx = ['pg', 'slotxo', 'ambslot', 'yeekee']
    // console.log(`${xxx}.${input} = ${xxx[input]}`)
    return xxx[input]
  }
}