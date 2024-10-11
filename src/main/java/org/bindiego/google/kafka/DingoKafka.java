package org.bindiego.google.kafka;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;
import org.bindiego.util.DingoStats;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.Flow.Subscription;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;


public class DingoKafka extends Thread {
    public DingoKafka() {
        // Instantiate or get the current Global config
        this.config = Config.getConfig();

        this.projectId = config.getProperty("google.projectid").toString();
        // this.projectId = ServiceOptions.getDefaultProjectId();
        this.topicId = config.getProperty("google.kafka.topic").toString();

    }

    class SendCallback implements Callback {
          public void onCompletion(RecordMetadata m, Exception e){
              if (e == null){
                logger.debug("Produced a message successfully.");
              } else {
                logger.error(e.getMessage());
              }
          }
    }

    @Override
    public void run() {
        Properties p = new Properties();

        try {
          p.load(new java.io.FileReader("conf/kafka-client.properties"));
        } catch (FileNotFoundException ex) {
          logger.error("kafka client configuration file not found", ex);
        } catch (IOException ex) {
          logger.error("IOException", ex);
        } catch (Exception ex) {
          logger.fatal("Error", ex);
        }

        KafkaProducer producer = new KafkaProducer(p);
        ProducerRecord message = new ProducerRecord(topicId, "key", "value");
        SendCallback callback = new SendCallback();
        producer.send(message,callback);
        producer.close();
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DingoKafka.class.getName());

    private PropertiesConfiguration config;

    private ExecutorService execPub;
    private BlockingQueue<Runnable> pubbq;

    private ExecutorService execSub;
    private BlockingQueue<Runnable> subbq;

    private String projectId;
    private String topicId;
    private String subscriptionId;
}