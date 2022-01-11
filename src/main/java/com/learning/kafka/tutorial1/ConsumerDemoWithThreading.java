package com.learning.kafka.tutorial1;

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

public class ConsumerDemoWithThreading {

    public static void main(String[] args) {
        new ConsumerDemoWithThreading().run();

    }
    private ConsumerDemoWithThreading(){

    }
    private void run(){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "my-fourth-application";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreading.class.getName());

        CountDownLatch latch=new CountDownLatch(1);
        //create a runnable which will be used by the thread below
        Runnable consumerRunnable=new ConsumerRunnable(bootstrapServers,groupId,topic,latch);
        //thread
        Thread consumerThread= new Thread(consumerRunnable);
        consumerThread.start();

        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            logger.info("Cauhgt shutdown hook");
            ((ConsumerRunnable) consumerRunnable).shutDown();
            try {
                latch.await();
            } catch (InterruptedException e) {

            }
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            logger.info("Application got interupted");
        } finally {
            logger.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable{
        private CountDownLatch latch;
        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        KafkaConsumer<String, String> consumer;
        Properties properties = new Properties();
        public ConsumerRunnable(
                String bootStrapServers,
                String groupId,
                String topic,
                CountDownLatch latch) {
            this.latch=latch;

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootStrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);;
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            consumer = new KafkaConsumer<String, String>(properties);

            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));
        }

        @Override
        public void run() {
            try {

                while(true){
                    ConsumerRecords<String, String> records =
                            consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                    for (ConsumerRecord<String, String> record : records){
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("WakeUp exception intercepted ..signaling close of consumer");
            } finally {
                consumer.close();
                //tell the main that we are done and finsih
                latch.countDown();
            }

        }

        public void shutDown(){
            //throws WakeUpException ..a expected exception to stop the consumer
            consumer.wakeup();
        }
    }
}