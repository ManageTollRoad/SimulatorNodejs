const KafkaProducerClient = require("../services/kafkaProducerSdk")
const readConfFile = require("../conf/conf")

const conf = readConfFile()

const createRandomData = (vehicleId) => {
    const options = conf.data
    const data = {
        vehicleId,
        type: options.type[Math.floor(Math.random() * options.type.length)],
        section: options.section[Math.floor(Math.random() * options.section.length)],
        vehicleType: options.vehicleType[Math.floor(Math.random() * options.vehicleType.length)],
        dayOfWeek: options.dayOfWeek[Math.floor(Math.random() * options.dayOfWeek.length)],
        hour: options.hour[Math.floor(Math.random() * options.hour.length)],
        dayType: options.dayType[Math.floor(Math.random() * options.dayType.length)],
    }

    return data;
}

const afterConnect = () => {
    console.log("Producer connected to kafka!");
    for (let i = 0; i < 10; i++) {
        kafkaProducer.sendMessage(conf.dataTopic, createRandomData(i + 1))
    }
}

const kafkaProducer = new KafkaProducerClient(
    conf.prefix,
    afterConnect,
    (topic, msgAsJson) => console.log(`Push to ${topic} this message: ${JSON.stringify(msgAsJson)}`)
)

kafkaProducer.connect()


