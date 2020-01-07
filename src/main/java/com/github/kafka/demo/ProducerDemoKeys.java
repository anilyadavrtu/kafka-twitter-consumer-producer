package com.github.kafka.demo;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * @author anil
 */
public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);
        System.out.println("Hello");
        String bootStrapServer = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        for (int i = 0; i < 10; i++) {
            //create producer record
            String topic = "first_topic";
            String value = "hello " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            ProducerRecord producerRecord = new ProducerRecord<String, String>(topic, key, value);
            logger.info("key :" + key);
            //create producer
            KafkaProducer producer = new KafkaProducer<String, String>(properties);
            producer.send(producerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //execute every time a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger.info("Received new metadata : \n" +
                                "Topic : " + recordMetadata.topic() + "\n" +
                                "Partition :" + recordMetadata.partition() + "\n" +
                                "OffSet : " + recordMetadata.offset() + "\n" +
                                "Timestamp :" + recordMetadata.timestamp());
                    } else {
                        logger.error("Error while producing", e.getMessage());
                    }
                }
            }).get();// block to send() to make it synchronous -don't do this in production.
            producer.flush();
            producer.close();
        }
    }
}
