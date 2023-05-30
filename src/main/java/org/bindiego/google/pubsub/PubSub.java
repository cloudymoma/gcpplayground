package org.bindiego.google.pubsub;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;
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

import io.grpc.Status.Code;

import com.google.common.collect.Lists;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.ApiException;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.protobuf.ByteString;

import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.SubscriptionName;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;

public class PubSub extends Thread {
    public PubSub() {
        // Instantiate or get the current Global config
        this.config = Config.getConfig();

        this.projectId = config.getProperty("google.projectid").toString();
        // this.projectId = ServiceOptions.getDefaultProjectId();
        this.topicId = config.getProperty("google.pubsub.topic").toString();
        this.subscriptionId = config.getProperty("google.pubsub.subscription").toString();

    }

    /**
     * Pre-setup using gcloud command
     * 
     * Create a topic, for example:
     * gcloud pubsub topics create dingoactions
     *
     * Create a subscription, for example:
     * gcloud pubsub subscriptions create --topic dingoactions dingoactions-sub
     */
    private void init(CredentialsProvider credentialsProvider) {
        logger.info("Setup the pubsub topic and subscription");

        logger.info("creating topic");
        TopicName topicName = TopicName.of(projectId, topicId);
        try {
            TopicAdminSettings topicAdminSettings =
                 TopicAdminSettings.newBuilder()
                     .setCredentialsProvider(credentialsProvider)
                     .build();
            TopicAdminClient topicAdminClient = TopicAdminClient.create(topicAdminSettings);
            Topic topic = topicAdminClient.createTopic(topicName);

            logger.info("Topic %s:%s created.\n", topicName.getProject(), topicName.getTopic());
        } catch (java.io.IOException ex) {
            logger.error("IOException", ex);
        } catch (ApiException ex) {
            if (ex.isRetryable())
                logger.debug("TODO: operation retryable");
            logger.error("Status code: " + ex.getStatusCode().getCode());
            logger.error("API Exception", ex);
        }

        logger.info("creating the subscription");
        try {
            SubscriptionAdminSettings subscriptionAdminSettings =
                SubscriptionAdminSettings.newBuilder()
                     .setCredentialsProvider(credentialsProvider)
                    .build();
            SubscriptionAdminClient subscriptionAdminClient = SubscriptionAdminClient.create(subscriptionAdminSettings);
            SubscriptionName subscriptionName = SubscriptionName.of(projectId, subscriptionId);
            // Create a pull subscription with default acknowledgement deadline of 10 seconds.
            // Messages not successfully acknowledged within 10 seconds will get resent by the server.
            Subscription subscription =
                subscriptionAdminClient.createSubscription(
                    subscriptionName, topicName, PushConfig.getDefaultInstance(), 10);

            logger.info(
                "Subscription %s:%s created.\n",
                subscriptionName.getProject(), subscriptionName.getSubscription());
        } catch (java.io.IOException ex) {
            logger.error("IOException", ex);
        } catch (ApiException ex) {
            if (ex.isRetryable())
                logger.debug("TODO: operation retryable");
            logger.error("Status code: " + ex.getStatusCode().getCode());
            logger.error("API Exception", ex);
        }
    }

    @Override
    public void run() {
        // Explicitly load google service account credentials
        String gCredentials = config.getProperty("google.credentials").toString();
        CredentialsProvider credentialsProvider = null;
        try {
            credentialsProvider = FixedCredentialsProvider.create(
                ServiceAccountCredentials.fromStream(
                    new FileInputStream(gCredentials)));
        } catch (Exception ex) {
            logger.fatal("Credential loading failed", ex);
        }

        if (!Boolean.valueOf(config.getProperty("google.pubsub.skip.init").toString())) {
            init(credentialsProvider);
        } else {
            logger.info("Skip creation of publication and subscriptions");
        }

        if (config.getProperty("google.pubsub.pub").toString().equalsIgnoreCase("on")) {
            // Setup the pub threading pool
            // pubbq = new ArrayBlockingQueue<Runnable>(128);
            // execPub = new ThreadPoolExecutor(2, 128, 60, TimeUnit.SECONDS, pubbq);

            // Run threads
            int numPubThreads = Integer.parseInt(
                config.getProperty("google.pubsub.pub.threads").toString());
            execPub = Executors.newFixedThreadPool(numPubThreads);
            
            for (int i = 0; i < numPubThreads; ++i) {
                execPub.execute(
                    new DoPub(
                        TopicName.of(projectId, topicId), 
                        credentialsProvider));
            }
        }

        if (config.getProperty("google.pubsub.sub").toString().equalsIgnoreCase("on")) {
            // Setup the sub threading pool
            // subbq = new ArrayBlockingQueue<Runnable>(128);
            // execSub = new ThreadPoolExecutor(2, 128, 60, TimeUnit.SECONDS, subbq);

            // Run threads
            int numSubThreads = Integer.parseInt(
                config.getProperty("google.pubsub.sub.threads").toString());
            execSub = Executors.newFixedThreadPool(numSubThreads);

            for (int i = 0; i < numSubThreads; ++i) {
                execSub.execute(
                    new DoSub(
                        ProjectSubscriptionName.of(projectId, subscriptionId),
                        credentialsProvider));
            }
        }

        if (config.getProperty("google.pubsub.pub").toString().equalsIgnoreCase("on"))
            execPub.shutdown();

        if (config.getProperty("google.pubsub.sub").toString().equalsIgnoreCase("on"))
            execSub.shutdown();
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(PubSub.class.getName());

    private PropertiesConfiguration config;

    private ExecutorService execPub;
    private BlockingQueue<Runnable> pubbq;

    private ExecutorService execSub;
    private BlockingQueue<Runnable> subbq;

    private String projectId;
    private String topicId;
    private String subscriptionId;
}
