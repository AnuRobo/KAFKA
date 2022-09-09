package guru.learningjournal.kafka.examples;

class AppConfigs {
    final static String applicationID = "HelloProducer";
    final static String bootstrapServers = "localhost:9092,localhost:9093";
    final static String topicName1 = "hello-producer-1";
    final static String topicName2 = "hello-producer-2";
    final static int numEvents = 2;
    // use this value to set TRANSACTIONAL_ID_CONFIG for the producer
    final static String transaction_id = "Hello-Producer-Trans";
}
