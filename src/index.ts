import * as Amqp from "amqp-ts";
import dotenv from 'dotenv';
import { parse } from 'csv-parse';
import * as fs from "fs";
import * as path from "path";
import config from './config.json';

dotenv.config();

//RABBITMQ CONNECTION VARIABLES
const AMQP_URL = process.env.AMQP_URL!;
const QUEUE_NAME = process.env.QUEUE_NAME!;
const EXCHANGE_NAME = process.env.EXCHANGE_NAME!;

//CSA FILE VARIABLES
const csvFilePath = path.join(__dirname, '..\\sensor.csv');
const headers = ['value'];
let currentLine = 0;

//create a connection to the RabbitMQ server
var connection = new Amqp.Connection(AMQP_URL);
var queue = connection.declareQueue(QUEUE_NAME);
var exchange = connection.declareExchange(EXCHANGE_NAME);
queue.bind(exchange);

console.log(csvFilePath)
//open the csv file
const fileContent = fs.readFileSync(csvFilePath, { encoding: 'utf-8' });

//On connection open, set the interval function to send data to queue
connection.completeConfiguration().then(() => {
    //send data to queue every X seconds
    setInterval(sendDataToQueue, config.dataFrequency);
});


// queue.activateConsumer((message) => {
//     console.log("Message received: " + message.getContent());
// });
 

const sendDataToQueue = () =>{
    //get current time stamp
    const timeStamp = new Date().getTime();
    //read the next line from the csv file
    parse(fileContent, { columns: headers, skip_empty_lines: true }, (err, records) => {
        //get the current line from the csv file
        const value = records[currentLine].value;
        currentLine++;
        //create a message object
        const message = {
            deviceID: config.deviceID,
            value: value,
            timestamp: timeStamp
        };
        //send the message to the queue
        exchange.send( new Amqp.Message(JSON.stringify(message)));
        console.log("Message sent");
    });
}
   