package org.bindiego.google.pubsub.lite;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;

import com.google.common.collect.ImmutableList;
import com.google.api.core.ApiFuture;
// import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.common.util.concurrent.MoreExecutors;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import org.threeten.bp.Duration;

import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsublite.CloudRegion;
import com.google.cloud.pubsublite.CloudZone;
import com.google.cloud.pubsublite.Partition;
import com.google.cloud.pubsublite.ProjectNumber;
import com.google.cloud.pubsublite.SubscriptionName;
import com.google.cloud.pubsublite.SubscriptionPath;
import com.google.cloud.pubsublite.cloudpubsub.FlowControlSettings;
import com.google.cloud.pubsublite.cloudpubsub.Subscriber;
import com.google.cloud.pubsublite.cloudpubsub.SubscriberSettings;

class DoSubLite implements Runnable {
    private DoSubLite() {}

    public DoSubLite(SubscriptionPath subscriptionPath, CredentialsProvider credentialsProvider){
        // Instantiate or get the current Global config
        this.config = Config.getConfig();
        this.subscriptionPath = subscriptionPath;
        this.credentialsProvider = credentialsProvider;
        this.partitionNumbers = ImmutableList.of(0);

        try {
            // The message stream is paused based on the maximum size or number of messages that the
            // subscriber has already received, whichever condition is met first.
            FlowControlSettings flowControlSettings =
                FlowControlSettings.builder()
                    // 10 MiB. Must be greater than the allowed size of the largest message (1 MiB).
                    .setBytesOutstanding(10 * 1024 * 1024L)
                    // 1,000 outstanding messages. Must be >0.
                    .setMessagesOutstanding(1000L)
                    .build();

            MessageReceiver receiver = 
                (PubsubMessage message, AckReplyConsumer consumer) -> {
                    // handle incoming message, then ack/nack the received message
                    logger.debug("\n------------\nMessage ID : " + message.getMessageId() + "\n" +
                    "Publish time seconds: " + message.getPublishTime()
                        .getSeconds() + "\n" +
                    "Publish time nanosecond: " + message.getPublishTime()
                        .getNanos() + "\n" +
                    "Data payload: " + message.getData().toStringUtf8() + "\n" +
                    "Attribute timestamp: " 
                        + message.getAttributesOrDefault("timestamp", "CANNOT get timestamp") + "\n" +
                    "Attribute ID: " 
                        + message.getAttributesOrDefault("id", "CANNOT get id") 
                        + "\n--------------\n");
                    consumer.ack();
                };

            List<Partition> partitions = new ArrayList<>();
            for (Integer num : partitionNumbers) {
                    partitions.add(Partition.of(num));
            }

            SubscriberSettings subscriberSettings =
                SubscriberSettings.newBuilder()
                    .setSubscriptionPath(this.subscriptionPath)
                    .setPartitions(partitions)
                    .setReceiver(receiver)
                    // Flow control settings are set at the partition level.
                    .setPerPartitionFlowControlSettings(flowControlSettings)
                    .build();
            this.subscriber = Subscriber.create(subscriberSettings);
        } catch (Exception ex) {
            logger.error("Failed to init subscriber", ex);
        }  

        logger.info("Subscriber %s initialized", Thread.currentThread().getName());
    }

    @Override
    public void run() {
        logger.info("Subscriber %s started", Thread.currentThread().getName());

        try {
            // Start the subscriber. Upon successful starting, its state will become RUNNING.
            subscriber.startAsync().awaitRunning();
            logger.info("Listening to messages on %s ...", this.subscriptionPath.toString());

            // Allow the subscriber to run indefinitely unless an unrecoverable error occurs
            // subscriber.awaitTerminated();

            // Wait 30 seconds for the subscriber to reach TERMINATED state. If it encounters
            // unrecoverable errors before then, its state will change to FAILED and an
            // IllegalStateException will be thrown.
            subscriber.awaitTerminated(30, TimeUnit.SECONDS);
        } catch (TimeoutException ex) {
            // Shut down the subscriber. This will change the state of the subscriber to TERMINATED.
            subscriber.stopAsync().awaitTerminated();
            logger.info("Subscriber is shut down: %s", subscriber.state());

            logger.info("Subscriber %s done", Thread.currentThread().getName());
        } catch (Exception ex) {
            logger.error("Error", ex);
        } /* finally {
            if (null != subscriber) {
                // When finished with the publisher, make sure to shutdown to free up resources.
                logger.info("Shutting down the subscriber");
                subscriber.stopAsync();
            }
        } */
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DoSubLite.class.getName());

    private PropertiesConfiguration config;
    
    private SubscriptionPath subscriptionPath;
    private Subscriber subscriber;

    private List<Integer> partitionNumbers;

    private CredentialsProvider credentialsProvider;
}
