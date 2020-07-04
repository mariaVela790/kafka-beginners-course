package com.github.maria.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKey {
    public static void main(String[] args) {

        // create logger for class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKey.class);

        // create producer properties
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

       for (int i = 0; i <= 20; i++){
           String topic = "first_topic";
           String message = "Hello World Message" + (i + 40);
           String key = "id_" + i;

           // create producer record
           ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, message);

           //send data
           producer.send(record, new Callback() {
               public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                   //execute every time a record is successfully sent or an exception is thrown
                   if (e == null) {
                       logger.info("Received new metadata. \n" +
                               "Topic: " + recordMetadata.topic() + "\n" +
                               "Partition: " + recordMetadata.partition() + "\n" +
                               "Offset: " + recordMetadata.offset() + "\n" +
                               "Timestamp: " + recordMetadata.timestamp());
                   }
                   else {
                       logger.error("Error while producing", e);
                   }
               }
           });

       }

       producer.flush();
        // close and flush producer to make sure data is published. Otherwise we will not see anything from the consumer
        producer.close();

        // Note: without closing and flushing we wouldn't see data because the call to send data is asynchronous
        // we could also use producer.flush() but it is better to close
    }
}
