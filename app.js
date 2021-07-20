const { publish } = require("./kafkaProduce");

const carSim = [
  {
    road: "5",
    color: "red",
  },
];
publish(carSim);
