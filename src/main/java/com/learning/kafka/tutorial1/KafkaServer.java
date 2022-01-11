package com.learning.kafka.tutorial1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

public class KafkaServer {
    Logger logger = LoggerFactory.getLogger(KafkaServer.class.getName());

    public static void main(String[] args) {
        new KafkaServer().run();

    }
    private KafkaServer(){

    }
    private void run(){
        CountDownLatch latch=new CountDownLatch(1);
        Runnable zooKeeperRunnable=new ZooKeeperRunnable();
        Thread zooKeeperThread= new Thread(zooKeeperRunnable);
        zooKeeperThread.start();

        try {
            logger.info("\n\n Satrting zookeeper!!!!!\n\n");
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }finally {
            logger.info("\nzookeeper clsoed\n");
        }
    }

    public class ZooKeeperRunnable implements Runnable{
        @Override
        public void run() {
            String[] args = new String[] {"/bin/bash", "-c", "kafka-server-start.sh","/home/rushikeshc/kafka/kafka_2.12-3.0.0/config/server.properties"};
            try {
                logger.info("\n\n!!!Zookeeper started!!!!!!!!!\n\n");
                Process proc = new ProcessBuilder(args).start();
                System.out.println(proc.toString());

            } catch (IOException e) {
                logger.info("Problem starting zookeeper server"+e);
            }

        }
    }
}
