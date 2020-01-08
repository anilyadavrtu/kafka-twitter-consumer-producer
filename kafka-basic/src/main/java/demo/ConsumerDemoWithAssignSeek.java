package demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author anil
 */
public class ConsumerDemoWithAssignSeek {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithAssignSeek.class);

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
        Properties properties = new Properties();
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-forth-application";
        String topic = "first_topic";
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        //properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        //create the consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);
        //Assign and seek are mostly used to replay data or fetch a specific message


        //assign
        long offSetToReadFrom = 15L;
        TopicPartition partitionToReadFrom = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partitionToReadFrom));
        //seek
        consumer.seek(partitionToReadFrom, offSetToReadFrom);
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFor = 0;
        //poll the new Data
        while (keepOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                numberOfMessagesReadSoFor+=1;
                logger.info("key " + record.key() + " Value " + record.value());
                logger.info("Partition " + record.partition() + " offset " + record.offset());
            }
            if(numberOfMessagesReadSoFor>=numberOfMessagesToRead){
                keepOnReading=false;//to exit the while loop
                break;
            }
            logger.info("Existing the application");
        }
    }
}

