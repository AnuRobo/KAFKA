package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;


public class HelloProducer {
    private static final Logger LOGGER = LogManager.getLogger();
    public static void main(String[] args){
        LOGGER.info("Creating Kafka Producer....");

        Properties props = new Properties();
        // purpose of the client id is to track the source of the message
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID); // Purpose of the client id is to track the source of the message
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // creating producer object
        KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        for(int i = 0; i < AppConfigs.numEvents; i++ ){
            producer.send(new ProducerRecord<>(AppConfigs.topicName, i, "Simple Message-"+i)); // The send method takes a ProducerRecord object and sends it to the Kafka cluster
        }

        LOGGER.info("Finished sending Messages. Closing Producer");

        producer.flush();
        producer.close(); // If you do not close the producer after sending all the required messages, you will leak the resources created by the producer
    }
}
