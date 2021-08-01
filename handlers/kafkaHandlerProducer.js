const KafkaProducerClient = require("../services/kafkaProducerSdk")
const readConfFile = require("../conf/conf")

const conf = readConfFile()

const createRandomData = (vehicleId) => {
    const options = conf.data
    const data = {
        vehicleId,
        type: options.type[Math.floor(Math.random() * options.type.length)],
        vehicleType: options.vehicleType[Math.floor(Math.random() * options.vehicleType.length)],
        dayOfWeek: options.dayOfWeek[Math.floor(Math.random() * options.dayOfWeek.length)],
        hour: options.hour[Math.floor(Math.random() * options.hour.length)],
        dayType: options.dayType[Math.floor(Math.random() * options.dayType.length)],
        section: options.section[Math.floor(Math.random() * options.section.length)],
    }

    return data;
}

const postData = () => {
    console.log("Producer connected to kafka!");
    for (let i = 0; i < 10; i++) {
        kafkaProducer.sendMessage(conf.dataTopic, createRandomData(i + 1))
    }
}

const postTrain = () => {
    console.log("Producer connected to kafka for training!");
    for (let i = 0; i < 200; i++) {
        kafkaProducer.sendMessage(conf.trainTopic, createRandomData(i + 1))
    }
}

// for train make it true, for data make it false
const isTrain = true;

const kafkaProducer = new KafkaProducerClient(
    conf.prefix,
    isTrain? postTrain : postData,
    (topic, msgAsJson) => console.log(`Push to ${topic} this message: ${JSON.stringify(msgAsJson)}`)
)


kafkaProducer.connect()


