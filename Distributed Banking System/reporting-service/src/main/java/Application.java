import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Application {

    //topic its part of

    private static List<String> TOPICS=  new ArrayList<>(List.of("suspicious-transactions", "valid-transactions"));
    //servers
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {
        Application kafkaConsumerApp = new Application();
        //consumer group name
        String consumerGroup = "Reporting Service";
        if (args.length == 1) {

            consumerGroup = args[0];
        }

        System.out.println("Consumer is part of consumer group " + consumerGroup);
        //create kafka consumer
        Consumer <String, Transaction> kafkaConsumer = kafkaConsumerApp.createKafkaConsumer(BOOTSTRAP_SERVERS, consumerGroup);
        //consume the topic and consumer name
        kafkaConsumerApp.consumeMessages(TOPICS, kafkaConsumer);
    }

    public static void consumeMessages(List<String> topics, Consumer<String, Transaction> kafkaConsumer) {
        //subscribe to the topic
        kafkaConsumer.subscribe((topics));
        //keep reading messages from topic
        while (true){

            //poll check to see if their is new message all the time, interval of 1 second. if nothing returned exit and poll again
            ConsumerRecords<String, Transaction> consumerRecords = kafkaConsumer.poll(Duration.ofSeconds(1));


            //if there is a message process them. print it out
            for(ConsumerRecord<String, Transaction> record: consumerRecords){

               recordTransactionForReporting(record.topic(), record.value());
            }

            //do processing

            //done processing
            kafkaConsumer.commitAsync();

        }
    }

    public static Consumer<String, Transaction> createKafkaConsumer(String bootstrapServers, String consumerGroup) {
        Properties properties = new Properties();

        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, Transaction.TransactionDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new KafkaConsumer<>(properties);


    }

    private static void recordTransactionForReporting(String topic, Transaction transaction) {
        // Print transaction information to the console
        // Print a different message depending on whether transaction is suspicious or valid

        if (topic.equalsIgnoreCase(TOPICS.get(0))){
            System.out.println("List of Suspicious transactions: " + transaction.toString());
        }
        else if(topic.equalsIgnoreCase(TOPICS.get(1))){
            System.out.println("List of Valid transactions: " + transaction.toString());
        }
    }

}
