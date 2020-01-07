package com.github.kafka.demo;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * @author anil
 */
public class ConsumerDemoWithThread {
    Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class);

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private void run() {
        String bootStrapServer = "127.0.0.1:9092";
        String groupId = "my-forth-application";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);
        Runnable consumerThreadRunnable = new ConsumerRunnable(latch, topic, bootStrapServer, groupId);
        Thread myThread = new Thread(consumerThreadRunnable);
        myThread.start();
        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(()->
        {
            logger.info("caught shutdown hook");
            ((ConsumerRunnable)consumerThreadRunnable).shutDown();
        }));
        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted");
        } finally {
            logger.info("Application is closing");
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        logger.info("application has exited");

    }

    private class ConsumerRunnable implements Runnable {
        Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class);
        private CountDownLatch latch;
        //create the consumer
        private KafkaConsumer<String, String> consumer;

        private ConsumerRunnable(CountDownLatch countDownLatch, String topic, String bootStrapServer, String groupId) {
            this.latch = countDownLatch;
            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            //create the consumer
            consumer = new KafkaConsumer(properties);
            //Subscribe the consumer to our topics
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            //poll the new Data
            try {


                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key " + record.key() + " Value " + record.value());
                        logger.info("Partition " + record.partition() + " offset " + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("received shutdown signal");
            } finally {
                consumer.close();
                //tell our main code we are done with the consumer
                latch.countDown();
            }
        }

        public void shutDown() {
            //wake is a special method to interrupt the consumer.poll
            //it will throw the exception WakeUpException
            consumer.wakeup();
        }
    }
}

