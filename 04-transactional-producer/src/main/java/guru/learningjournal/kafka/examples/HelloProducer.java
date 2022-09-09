package guru.learningjournal.kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class HelloProducer {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // Setting a TRANSACTIONAL_ID for the producer is a mandatory requirement
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, AppConfigs.transaction_id);

        /*
        * Two Critical points to remember
        * 1 - When you set the transaction id, idempotence is automatically enabled because transaction are dependent on idempotence
        * 2 - TRANSACTIONAL_ID_CONFIG must be unique for each producer instance
        * The primary purpose of the transactional id is to rollback the older unfinished transaction for the same transaction id in case of producer application bounces or restarts
        * */

        /*
        * Implementing transaction in the producer is a three-step process
        * 1 - Initialize the transaction by calling initTransactions()
        * */
        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        // This method performs the necessary check to ensure that any other transaction initiated by previous instances of the same producer is closed
        // That means, if an application instance dies the next instance can be guranteed that any unfinished transactions have been either completed or aborted
        // leaving the new instance in a clean state before resuming the work
        // It also retrieves an internal producer_id, that will be used in all future messages sent by producer
        // The producer_id is used by the broker to implement idempotence
        // The next step is to wrap all your send() API calls within a pair of beginTransaction() and commitTransaction()
        producer.initTransactions();

        logger.info("Start First Transaction...");
        // All messages sent between the beginTransaction() and commitTransaction() will be part of a single transaction
        producer.beginTransaction();
        try { // in case you receive an exception that you can't recover from, abort the transaction and finally close the producer instance
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-1" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-1" + i));
            }
            logger.info("Committing First Transaction...");
            producer.commitTransaction();
        }catch(Exception e){
            logger.info("Exception in First Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        // For RollBack scenario
        logger.info("Start Second Transaction...");
        producer.beginTransaction();
        try {
            for (int i = 1; i <= AppConfigs.numEvents; i++) {
                producer.send(new ProducerRecord<>(AppConfigs.topicName1, i, "Simple Message-2" + i));
                producer.send(new ProducerRecord<>(AppConfigs.topicName2, i, "Simple Message-2" + i));
            }
            logger.info("Aborting Second Transaction...");
            producer.abortTransaction();
        }catch(Exception e){
            logger.info("Exception in Second Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        logger.info("Finished - Closing Kafka Producer.");
        producer.close();

    }
}

/*
* ONE FINAL NOTE ABOUT TRANSACTIONS
* The same producer cannot have multiple open transactions.
* You must commit or abort the transaction before you can begin a new one
* The commitTransaction() will flush any unsent records before committing the transaction
* If any of the send calls failed with an irrecoverable error that means even if a single message is not successfully delivered to Kafka the commitTransaction() will throw the exception and you are supposed to abort the whole transaction
*
* In a multiThreaded producer implementation, you will call the send() API from different threads. However you must call the beginTransaction() before starting those threads and either commit or abort when all the threads are complete
* */