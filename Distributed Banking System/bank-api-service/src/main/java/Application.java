import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Banking API Service
 */
public class Application {
    private static final String TOPIC1 = "suspicious-transactions";
    private static final String TOPIC2 = "valid-transactions";
    private static final String TOPIC3 = "high-value-transactions";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093,localhost:9094";


    public static void main(String[] args) {
        
        IncomingTransactionsReader incomingTransactionsReader = new IncomingTransactionsReader("user-transactions.txt");
        CustomerAddressDatabase customerAddressDatabase = new CustomerAddressDatabase("user-residence.txt");

        Producer<String, Transaction> kafkaProducer = createKafkaProducer(BOOTSTRAP_SERVERS);
        try {
            processTransactions(incomingTransactionsReader, customerAddressDatabase, kafkaProducer);
        } catch (ExecutionException | InterruptedException | FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            kafkaProducer.flush();
            kafkaProducer.close();
        }
    }


    public static void processTransactions(IncomingTransactionsReader incomingTransactionsReader,
                                           CustomerAddressDatabase customerAddressDatabase,
                                           Producer<String, Transaction> kafkaProducer) throws ExecutionException, InterruptedException, FileNotFoundException {

        // Retrieve the next transaction from the IncomingTransactionsReader
        // For the transaction user, get the user residence from the UserResidenceDatabase
        // Compare user residence to transaction location.
        // Send a message to the appropriate topic, depending on whether the user residence and transaction x
        // location match or not. x
        // Print record metadata information -

        while (incomingTransactionsReader.hasNext()) {

            //incoming transaction reader
            Transaction trns = incomingTransactionsReader.next();

            String residence = customerAddressDatabase.getUserResidence(trns.getUser());


             if (trns.getTransactionLocation().equalsIgnoreCase(residence)) {


                //send to valid-transaction
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC2, trns.getUser(), trns);

                //send record to kafka
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();

                System.out.println((String.format("Record with (key: %s, value: %s) was sent to (partition: %d, offset: %d",
                        record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset())));

            } else {

                //send to suspicious topic
                ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC1, trns.getUser(), trns);
                //send record to kafka
                RecordMetadata recordMetadata = kafkaProducer.send(record).get();

                System.out.println((String.format("Record with (key: %s, value: %s) was sent to (partition: %d, offset: %d",
                        record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset())));
            }

             if(trns.getAmount() > 1000 && trns.getTransactionLocation().equalsIgnoreCase(residence)){
                 //send to valid-transaction
                 ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC3, trns.getUser(), trns);

                 //send record to kafka
                 RecordMetadata recordMetadata = kafkaProducer.send(record).get();

                 System.out.println((String.format("Record with (key: %s, value: %s) was sent to (partition: %d, offset: %d",
                         record.key(), record.value(), recordMetadata.partition(), recordMetadata.offset())));

             }


        }

    }

    public static Producer<String, Transaction> createKafkaProducer(String bootstrapServers) {
        //configure producer
        Properties properties = new Properties();
        //key values
        //Location of bootstrap server
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        //client id config
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "events-producer");
        //tell kafka how to serialize key and get name
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //value
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Transaction.TransactionSerializer.class.getName());

        return new KafkaProducer<String, Transaction>(properties);
    }

}
