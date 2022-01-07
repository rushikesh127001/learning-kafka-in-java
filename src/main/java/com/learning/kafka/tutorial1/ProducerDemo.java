package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {
        //producer config
        String bootstrapServer="localhost:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //init producer with properties
        KafkaProducer<String,String> producer= new KafkaProducer<>(properties);

        //create producer records
        ProducerRecord<String,String> record=
                new ProducerRecord<>("first_topic", "hello from JAVA");

        //produce
        producer.send(record);
        //bt this is asynchronous
        producer.flush();
        producer.close();


    }
}
