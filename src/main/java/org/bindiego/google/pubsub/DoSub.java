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
            FlowControlSettings flowControlSettings =
                FlowControlSettings.newBuilder()
                    .setMaxOutstandingElementCount(10_000L)
                    .setMaxOutstandingRequestBytes(1_000_000_000L)
                    .build();

            MessageReceiver receiver = 
                new MessageReceiver() {
                    @Override
                    public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
                        // handle incoming message, then ack/nack the received message
                        logger.debug("Id : " + message.getMessageId());
                        logger.debug("Data : " + message.getData().toStringUtf8());
                        consumer.ack();
                    }};
                                                   
            subscriber = Subscriber.newBuilder(subscriptionName, receiver)
                .setFlowControlSettings(flowControlSettings)
                .setCredentialsProvider(this.credentialsProvider)
                .build();

            subscriber.startAsync().awaitRunning();
            // Allow the subscriber to run indefinitely unless an unrecoverable error occurs
            subscriber.awaitTerminated();
        } catch (Exception ex) {
            logger.error("Error", ex);
        } finally {
            if (null != subscriber) {
                // When finished with the publisher, make sure to shutdown to free up resources.
                logger.info("Shutting down the subscriber");
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
