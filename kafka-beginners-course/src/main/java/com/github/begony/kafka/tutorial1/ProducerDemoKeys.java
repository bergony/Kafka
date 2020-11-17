package com.github.begony.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;


public class ProducerDemoKeys {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

        String bootstrapServers = "127.0.0.1:9092";

        //Create Producer Properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for (int i=0; i < 10; i++) {
            //create a producer record

            String topic = "first_topic";
            String value = "Hello word" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);


            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic,value );


            logger.info("Key: "+ key);

            //seed data - asychronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes every time  a record is successfully sent or excetoion  is throw
                    if (e == null) {
                        // The record was Successfully sent
                        logger.info("Received new metadata, \n" +
                                "Topic:" + recordMetadata.topic() + "\n" +
                                "Paratition:" + recordMetadata.partition() + "\n" +
                                "Offset:" + recordMetadata.offset() + "\n" +
                                "Timestamp:" + recordMetadata.timestamp());

                    } else {
                        logger.error("Error while producing", e);
                    }
                }
            }).get(); // Block the .send() to make it synchronous - don't do this on produção!
        }

        //flush data
        producer.flush();

        // flush and close producer
        producer.close();

    }

}
