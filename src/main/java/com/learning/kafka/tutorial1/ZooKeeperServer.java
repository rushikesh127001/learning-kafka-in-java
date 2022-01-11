package com.learning.kafka.tutorial1;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.CountDownLatch;

public class ZooKeeperServer{
    Logger logger = LoggerFactory.getLogger(ZooKeeperServer.class.getName());

    public static void main(String[] args) {
        new ZooKeeperServer().run();

    }
    private ZooKeeperServer(){

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
            String[] args = new String[] {"/bin/bash", "-c", "zookeeper-server-start.sh /home/rushikeshc/kafka/kafka_2.12-3.0.0/config/zookeeper.properties"};
            try {
                logger.info("\n\n!!!Zookeeper started!!!!!!!!!\n\n");
                Process proc = new ProcessBuilder(args).start();

                BufferedReader reader =
                        new BufferedReader(new InputStreamReader(proc.getInputStream()));
                StringBuilder builder = new StringBuilder();
                String line = null;
                while ( (line = reader.readLine()) != null) {
                    builder.append(line);
                    builder.append(System.getProperty("line.separator"));
                }
                String result = builder.toString();
                logger.info(result);proc.waitFor();

//                proc.getOutputStream().flush();
//                //System.out.println(proc.toString());

            } catch (IOException e) {
                logger.info("Problem starting zookeeper server"+e);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

        }
    }
}
