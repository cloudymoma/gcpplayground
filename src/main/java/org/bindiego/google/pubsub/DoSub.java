package org.bindiego.google.pubsub;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;

import com.google.api.core.ApiFuture;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.threeten.bp.Duration;

class DoSub implements Runnable {
    private DoSub() {}

    public DoSub(ProjectSubscriptionName subscriptionName, CredentialsProvider credentialsProvider){
        // Instantiate or get the current Global config
        this.config = Config.getConfig();
        this.subscriptionName = subscriptionName;
        this.credentialsProvider = credentialsProvider;
    }

    @Override
    public void run() {
        try {
            // The subscriber will pause the message stream and stop receiving more messsages from the
            // server if any one of the conditions is met.
            FlowControlSettings flowControlSettings =
                FlowControlSettings.newBuilder()
                    // 1,000 outstanding messages. Must be >0. It controls the maximum number of messages
                    // the subscriber receives before pausing the message stream.
                    .setMaxOutstandingElementCount(1000L)
                    // 100 MiB. Must be >0. It controls the maximum size of messages the subscriber
                    // receives before pausing the message stream.
                    .setMaxOutstandingRequestBytes(100L * 1024L * 1024L)
                    .build();

            // Provides an executor service for processing messages.
            ExecutorProvider executorProvider =
                InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(4).build();

            //TODO: https://github.com/googleapis/java-pubsub/blob/52263ce63d4cbda649121e465f4bdc78bbfa8e44/samples/snippets/src/main/java/pubsub/SubscribeWithExactlyOnceConsumerWithResponseExample.java
            MessageReceiver receiver = 
                new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                        // handle incoming message, then ack/nack the received message
                        logger.info("\n------------\nMessage ID : " + message.getMessageId() + "\n" +
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

                        logger.info("Custom Attributes: ");
                        message
                            .getAttributesMap()
                            .forEach((key, value) -> logger.info(key + " = " + value));

                        consumer.ack();
                    }};
                                                   
            subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                .setFlowControlSettings(flowControlSettings)
                .setCredentialsProvider(this.credentialsProvider)
                .setMaxAckExtensionPeriod(Duration.ofSeconds(120))
                .setParallelPullCount(2)
                .setExecutorProvider(executorProvider)
                .build();

            // Listen for unrecoverable failures. Rebuild a subscriber and restart subscribing
            // when the current subscriber encounters permanent errors.
            subscriber.addListener(
                new Subscriber.Listener() {
                    public void failed(Subscriber.State from, Throwable failure) {
                        System.out.println(failure.getStackTrace());
                        if (!executorProvider.getExecutor().isShutdown()) {
                           run();
                        }
                    }
                },
                MoreExecutors.directExecutor());

            subscriber.startAsync().awaitRunning();
            logger.info("Listening for messages on %s:", subscriptionName.toString());

            // Allow the subscriber to run indefinitely unless an unrecoverable error occurs
            // subscriber.awaitTerminated();
            subscriber.awaitTerminated(10, TimeUnit.SECONDS);
        } catch (TimeoutException timeoutException) {
            logger.error("TimeoutException", timeoutException);
        } catch (Exception ex) {
            logger.error("Error", ex);
        } finally {
            if (null != subscriber) {
                // When finished with the publisher, make sure to shutdown to free up resources.
                logger.warn("Shutting down the subscriber");
                subscriber.stopAsync();
            }
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DoSub.class.getName());

    private PropertiesConfiguration config;
    
    private Subscriber subscriber;
    private ProjectSubscriptionName subscriptionName;
    private CredentialsProvider credentialsProvider;
}
