package guru.learningjournal.kafka.examples;

import guru.learningjournal.kafka.examples.serde.JsonDeserializer;
import guru.learningjournal.kafka.examples.serde.JsonSerializer;
import guru.learningjournal.kafka.examples.types.PosInvoice;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class PostValidator {
    /*
    * It Implements a Consumer-transform-produce pipeline
    * In this pipeline, we consume invoices
    * Identify if they are valid or invalid
    * and produce the valid invoices to a Kafka topic and invalid ones to a different topic
    * */

    // Returns a Logger with the name of the calling class.
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties Consumerprops = new Properties();
        // Step 1 - CONSUMER CONFIGURATIONS
        /*
        * CLIENT ID
        * An optional identifier of a Kafka consumer (in a consumer group) that is passed to a Kafka broker with every request.
        * The sole purpose of this is to be able to track the source of requests beyond just ip and port by allowing a logical application name to be included in Kafka logs and monitoring aggregates.
        * _____________________________________________________________________________________________________________________________________________________________________________________________________________________________________
        * A logical identifier of an application. Can be used by brokers to apply quotas or trace requests to a specific application
        * _____________________________________________________________________________________________________________________________________________________________________________________________________________________________________
        * Client-id is a logical grouping of clients with a meaningful name chosen by the client application. The tuple (user, client-id) defines a secure logical group of clients that share both user principal and client-id.
        * Quotas can be applied to (user, client-id), user or client-id groups.
        * _____________________________________________________________________________________________________________________________________________________________________________________________________________________________________
        * An id string to pass to the server when making requests.
        * The purpose of this is to be able to track the source of requests beyond just ip/port by allowing a logical application name to be included in server-side request logging.
        * */
        Consumerprops.put(ConsumerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        Consumerprops.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        Consumerprops.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        Consumerprops.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        // VALUE_CLASS_NAME_CONFIG is the target deserialized Java Class name
        // We want our message to be deserialized to a POSInvoice
        Consumerprops.put(JsonDeserializer.VALUE_CLASS_NAME_CONFIG, PosInvoice.class);
        Consumerprops.put(ConsumerConfig.GROUP_ID_CONFIG, AppConfigs.groupID);
        Consumerprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        /*
        * When you write .class after a class name, it references the class literal - java.lang.Class object that represents information about given class.
        * For example, if your class is Print, then Print.class is an object that represents the class Print on runtime. It is the same object that is returned by the getClass() method of any (direct) instance of Print.
        * .class is used when there isn't an instance of the class available.
        * .getClass() is used when there is an instance of the class available.
        * object.getClass() returns the class of the given object.
        * ---------------------------------------------------------------------------------------------------
        * This '.class' method is used in Java for code Reflection. Generally you can gather meta data for your class such as the full qualified class name, list of constants, list of public fields, etc, etc.
        * */
        // Step 2 - CREATED CONSUMER
        KafkaConsumer<String, PosInvoice> consumer = new KafkaConsumer<String, PosInvoice>(Consumerprops);
        // Step 3 - Subscribe to the topic
        // Subscribe to the topics that we want to read
        // A Kafka consumer can subscribe to a list of topics
        consumer.subscribe(Arrays.asList(AppConfigs.sourceTopicNames));


        // KAFKA PRODUCER
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        KafkaProducer<String, PosInvoice> producer = new KafkaProducer<String, PosInvoice>(producerProps);


        // Step 4 -
        while(true){
            /*
            * The poll API is designed to ensure consumer liveness.
            * As long as you continue to call poll, the consumer will stay in the group and continue to receive messages from the partitions it was assigned.
            * Underneath the covers, the consumer sends periodic heartbeats to the server.
            * If the consumer crashes or is unable to send heartbeats for a duration of session.timeout.ms, then the consumer will be considered dead and its partitions will be reassigned.
            * */

            // The poll() method will immediately return an iterable ConsumerRecords
            // If there are no records at the broker, it will wait for the timeout
            // When the timeout expires an empty ConsumerRecords will be returned
            ConsumerRecords<String, PosInvoice> records = consumer.poll(Duration.ofMillis(100));

            for(ConsumerRecord<String, PosInvoice> record:records){
                // we want to send these invalid and valid records to separate Kafka topics. And would require creating a producer
                if(record.value().getDeliveryType().equals("HOME-DELIVERY") && record.value().getDeliveryAddress().getContactNumber().equals("")){
                    // Invalid
                    producer.send(new ProducerRecord<>(AppConfigs.invalidTopicName,record.value().getStoreID(),record.value()));
                    logger.info("invalid record - " + record.value().getInvoiceNumber());
                }else{
                    // Valid
                    producer.send(new ProducerRecord<>(AppConfigs.validTopicName,record.value().getStoreID(),record.value()));
                    logger.info("valid record - " + record.value().getInvoiceNumber());
                }
            }
        }
    }
}
