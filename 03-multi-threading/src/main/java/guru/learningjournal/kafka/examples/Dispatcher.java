package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class Dispatcher implements Runnable{

    private static final Logger LOGGER = LogManager.getLogger();
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer, String> producer;

    public Dispatcher(KafkaProducer<Integer, String> producer, String fileLocation, String topicName) {
        this.producer = producer;
        this.fileLocation = fileLocation;
        this.topicName = topicName;
    }

    @Override
    public void run(){
        LOGGER.info("Start Processing " + fileLocation);

        File file = new File(fileLocation);
        int counter = 0;
        try{
            Scanner scanner = new Scanner(file);
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName,null,line));
                counter++;
            }
            LOGGER.info("Finished sending " + counter + " message from " + fileLocation);
        }catch(FileNotFoundException e){
            throw new RuntimeException(e);
        }
    }
}
