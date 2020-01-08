package demo;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author anil
 */
public class ProducerDemo {
    final Logger logger = LoggerFactory.getLogger(ProducerDemo.class);
    public static void main(String[] args) {

        System.out.println("Hello");
        String bootStrapServer = "127.0.0.1:9092";
        //create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        for (int i = 0; i < 10; i++) {
            //create producer record
            ProducerRecord producerRecord = new ProducerRecord<String, String>("first_topic", "hello " + Integer.toString(i));
            //create producer
            KafkaProducer producer = new KafkaProducer<String, String>(properties);
            producer.send(producerRecord);
            producer.flush();
            producer.close();
        }
    }
}
