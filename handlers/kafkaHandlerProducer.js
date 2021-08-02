const KafkaProducerClient = require("../services/kafkaProducerSdk")
const readConfFile = require("../conf/conf")
const oneSection = 1
const secondSection = 2;
const thirdSection = 3
const fifthSection = 5
const fourSection = 4
const ENTER_ROAD = 0
const EXIT_ROAD = 1
const ENTER_SECTION = 3
const EXIT_SECTION = 2

const conf = readConfFile()
const options=  conf.data

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

const enterOrExitSection = (vehicleId,sectionNumber,operation) => {
    const options = conf.data
    const data = {
        vehicleId,
        type: options.type[operation],
        vehicleType: options.vehicleType[Math.floor(Math.random() * options.vehicleType.length)],
        dayOfWeek: options.dayOfWeek[Math.floor(Math.random() * options.dayOfWeek.length)],
        hour: options.hour[Math.floor(Math.random() * options.hour.length)],
        dayType: options.dayType[Math.floor(Math.random() * options.dayType.length)],
        section: options.section[sectionNumber],
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
    

//enter from section one and exit from fifth section 
    
  
    for (let i = 0; i < 80; i++) {
        //enter to section one
        const data = enterOrExitSection((i + 1),oneSection,ENTER_ROAD);
        setTimeout( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),1000)
        //enter to section 2
        data.section = options.section[secondSection]
        setTimeout(( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),1500))
        //enter to section 3
        data.section = options.section[thirdSection]
        setTimeout(( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),2000))
        //enter to section 4
        data.section = options.section[fourSection]
        setTimeout(( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),3000))
        
        // exit from section 5
        setTimeout( kafkaProducer.sendMessage.bind(conf.trainTopic, enterOrExitSection((i + 1),fifthSection,EXIT_ROAD)),4000)
    }

 
//enter from one section and exit from four section

   
    for (let i = 80; i < 160; i++) {
         //enter to section one
        const data = enterOrExitSection((i + 1),oneSection,ENTER_ROAD);
        setTimeout( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),1000)
        //enter to section 2
        data.section = options.section[secondSection]
        setTimeout(( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),1500))
        //enter to section 3
        data.section = options.section[thirdSection]
        setTimeout(( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),2000))
        // exit from section 5
        setTimeout( kafkaProducer.sendMessage.bind(conf.trainTopic, enterOrExitSection((i + 1),fourSection,EXIT_ROAD)),4000)
    }

//enter from section two and exit from fifth section

for (let i = 160; i < 200; i++) {
    //enter to section 2
    const data = enterOrExitSection((i + 1),secondSection,ENTER_ROAD);
    setTimeout( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),1000)
    //enter to section 3
    data.section = options.section[thirdSection]
    setTimeout(( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),2000))
    //enter to section 4
    data.section = options.section[fourSection]
    setTimeout(( kafkaProducer.sendMessage.bind(conf.trainTopic, data ),3000))
    // exit from section 5
    setTimeout( kafkaProducer.sendMessage.bind(conf.trainTopic, enterOrExitSection((i + 1),fifthSection,EXIT_ROAD)),4000)
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


