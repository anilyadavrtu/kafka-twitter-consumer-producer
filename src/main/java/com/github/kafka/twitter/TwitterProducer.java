package com.github.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

/**
 * @author anil
 */
public class TwitterProducer {
    Logger logger = LoggerFactory.getLogger(TwitterProducer.class);
    String consumerKey = "IsPJeZKSomZbB0BR3l0OFjuUO";
    String consumerSecret = "FTAAod7qf1FsSDqc10P4HhJ7Lz1Uf9caCjJXOVaXomyeozbauK";
    String token = "746868031-chmeriZNVkP3WHV1RuB9pHNUPElgpIXTFu0gBaFX";
    String tokenSecret = "z8Zc5ZDe0l8iLj1rPvXF42BQqbKTcnYIbsuUw0g1NSXTB";
    List<String> terms = Lists.newArrayList("kafka");

    public static void main(String[] args) {
        System.out.println("Hello");
        new TwitterProducer().run();
    }

    public void run() {
        /** set up your blocking queue : be sure to size these property TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingDeque<String>(1000);
        //create twitter clients
        Client client = createTwitterClient(msgQueue);
        client.connect();
        //create a kafka producer
       KafkaProducer<String ,String> producer=createKafkaProducer();

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        if(e!=null){
                            logger.error("some bad happen",e);
                        }
                    }
                });
            }

        }
        logger.info("End of Application");

    }

    public Client createTwitterClient(BlockingQueue<String> msgQueue) {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        List<String> terms = Lists.newArrayList("INDIA");
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, tokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        // Attempts to establish a connection.

        return hosebirdClient;
    }
    public KafkaProducer<String,String> createKafkaProducer(){
        String bootStrapServer = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //create safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        KafkaProducer producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}
