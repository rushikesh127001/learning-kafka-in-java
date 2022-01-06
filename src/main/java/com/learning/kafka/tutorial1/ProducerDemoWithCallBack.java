package com.learning.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithCallBack {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Logger logger= LoggerFactory.getLogger(ProducerDemoWithCallBack.class);//LoggerFactory.getLogger(ProducerDemoWithCallBack.class);
        //producer config
        String bootstrapServer="localhost:9092";
        Properties properties=new Properties();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //init producer with properties
        KafkaProducer<String,String> producer= new KafkaProducer<>(properties);

        //create producer records
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String,String> record=
                    new ProducerRecord<>("first_topic", "ID_"+Integer.toString(i),"hello from JAVA"+" -"+Integer.toString(i));
            logger.info("\n"+Integer.toString(i)+"is key\n");
            producer.send(record, (recordMetadata, e) -> {
                if(e==null){
                    logger.info(
                            "\n Logging Producer DETAIlS:"+"\n"
                                    +"Topic: "+recordMetadata.topic()+"\n"
                                    +"Partitiond: "+recordMetadata.partition()+"\n"
                                    +"Offset: "+recordMetadata.offset()+"\n\n"
                    );
                }
                else {
                    logger.error("!!!!!!!!!!!!!!!!!!!Error occured");

                }
            }).get();
        }


        //produce
        logger.info("HELOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO");


        //bt this is asynchronous
        producer.flush();
        producer.close();


    }
}
