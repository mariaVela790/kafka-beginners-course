package com.github.maria.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
//        create producer properties
        Properties properties = new Properties();

        // set properties
        String bootstrapServer = "localhost:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        // kafka converts whatever we send (a string) as bytes so we need a string serializer
        // Kafka provides a string serializer for this purpose
        // We also use the ProducerConfig class to use static strings for property names so the code is cleaner to read
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer by creating producer object using properties and kafka producer constructor
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        // create producer record
        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "Hello World Message");

        //send data
        producer.send(record);

        // close and flush producer to make sure data is published. Otherwise we will not see anything from the consumer
        producer.close();

        // Note: without closing and flushing we wouldn't see data because the call to send data is asynchronous
        // we could also use producer.flush() but it is better to close
    }
}
