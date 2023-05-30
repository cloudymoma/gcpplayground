package org.bindiego.google.pubsub;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.batching.FlowController.LimitExceededBehavior;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Timestamp.Builder;
import com.google.pubsub.v1.TopicName;
import com.google.pubsub.v1.PubsubMessage;
import com.google.common.util.concurrent.MoreExecutors;
import java.io.FileInputStream;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.List;
import java.util.ArrayList;
import java.util.Random; 
import org.threeten.bp.Duration;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

class DoPub implements Runnable {
    private DoPub() {}

    public DoPub(TopicName topicName, CredentialsProvider credentialsProvider){
        // Instantiate or get the current Global config
        config = Config.getConfig();

        try {
            // Configure how many messages the publisher client can hold in memory
            // and what to do when messages exceed the limit.
            FlowControlSettings flowControlSettings =
                FlowControlSettings.newBuilder()
                    // Block more messages from being published when the limit is reached. The other
                    // options are Ignore (or continue publishing) and ThrowException (or error out).
                    .setLimitExceededBehavior(LimitExceededBehavior.Block)
                    .setMaxOutstandingRequestBytes(10 * 1024 * 1024L) // 10 MiB
                    .setMaxOutstandingElementCount(100L) // 100 messages
                    .build();
            // batch settings
            long requestBytesThreshold = 5000L; // default : 1 byte
            long messageCountBatchSize = 10L; // default : 1 message

            Duration publishDelayThreshold = Duration.ofMillis(100); // default : 1 ms
            
            // Publish request get triggered based on request size, messages count & time since last publish
            BatchingSettings batchingSettings =
                BatchingSettings.newBuilder()
                    .setElementCountThreshold(messageCountBatchSize)
                    .setRequestByteThreshold(requestBytesThreshold)
                    .setDelayThreshold(publishDelayThreshold)
                    .setFlowControlSettings(flowControlSettings)
                    .build();

            // retry settings
            Duration retryDelay = Duration.ofMillis(100); // default: 100 ms
            double retryDelayMultiplier = 2.0; // back off for repeated failures, default: 1.3
            Duration maxRetryDelay = Duration.ofSeconds(60); // default : 1 minute
            Duration initialRpcTimeout = Duration.ofSeconds(1); // default: 5 seconds
            double rpcTimeoutMultiplier = 1.0; // default: 1.0
            Duration maxRpcTimeout = Duration.ofSeconds(600); // default: 10 minutes
            Duration totalTimeout = Duration.ofSeconds(600); // default: 10 minutes
            RetrySettings retrySettings =
                RetrySettings.newBuilder()
                    .setInitialRetryDelay(retryDelay)
                    .setRetryDelayMultiplier(retryDelayMultiplier)
                    .setMaxRetryDelay(maxRetryDelay)
                    .setInitialRpcTimeout(initialRpcTimeout)
                    .setRpcTimeoutMultiplier(rpcTimeoutMultiplier)
                    .setMaxRpcTimeout(maxRpcTimeout)
                    .setTotalTimeout(totalTimeout)
                    .build();

            // Provides an executor service for processing messages. The default
            // `executorProvider` used by the publisher has a default thread count of
            // 5 * the number of processors available to the Java virtual machine.
            ExecutorProvider executorProvider =
                InstantiatingExecutorProvider.newBuilder()
                    .setExecutorThreadCount(4)
                    .build();

            publisher = Publisher.newBuilder(topicName)
                .setCredentialsProvider(credentialsProvider)
                .setBatchingSettings(batchingSettings)
                .setRetrySettings(retrySettings)
                .setExecutorProvider(executorProvider)
                .build();
        } catch (Exception ex) {
            logger.error("Failed to init publisher", ex);
        }
    }

    @Override
    public void run() {
        try {
            // compile a message delimited by comma
            // timestamp,thread_id,thread_name,order_num
            final String deli = ",";
            final Thread currentThread = Thread.currentThread();
            final String threadName = currentThread.getName();
            final long threadId = currentThread.getId();

            // loop control, number of messages to be sent
            int numLoops = Integer.parseInt(
                config.getProperty("google.pubsub.pub.threads.msgnum").toString());

            // read Firebase sample Json data
            Scanner scanner = new Scanner(new File(
                config.getProperty("firebase.sample.data").toString()));
            List<String> fb_samples = new ArrayList<String>();
            while (scanner.hasNextLine()) {
				fb_samples.add(scanner.nextLine());
			}
			scanner.close();
            int modulor = fb_samples.size() - 1;

            // Publish messages
            for (int i = 0; i < numLoops; ++i) {
                final long millis = System.currentTimeMillis();

                final String message = fb_samples.get(i % modulor);

                final String msgId = UUID.randomUUID().toString();

                ByteString data = ByteString.copyFromUtf8(message);

                PubsubMessage pubsubMessage = 
                    PubsubMessage.newBuilder()
                        .setData(data)
                        .setPublishTime(
                            Timestamp.newBuilder().setSeconds(millis / 1000) 
                                 .setNanos((int) ((millis % 1000) * 1000000)).build()
                        )
                        .setMessageId(msgId)
                        .putAttributes("id", msgId) 
                        .putAttributes("timestamp", Long.toString(millis)) // Exact Java Milli ¯\_(ツ)_/¯
                        .build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                // Add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(
                    future,
                    new ApiFutureCallback<String>() {

                    @Override
                    public void onFailure(Throwable throwable) {
                        if (throwable instanceof ApiException) {
                            ApiException apiException = ((ApiException) throwable);
                            // details on the API exception
                            logger.error(apiException.getStatusCode().getCode());
                            logger.error(apiException.isRetryable());
                        }
                        logger.error("Error publishing message : " + message);
                    }

                    @Override
                    public void onSuccess(String messageId) {
                        // Once published, returns server-assigned message ids (unique within the topic)
                        logger.info("Published message ID: " + messageId);
                    }
                    },
                    MoreExecutors.directExecutor());
            }

        } catch (FileNotFoundException ex) {
			logger.error("Error", ex);
        } catch (Exception ex) {
            logger.error("Error", ex);
        } finally {
            // showdown
            if (null != publisher) {
                try {
                    // When finished with the publisher, make sure to shutdown to free up resources.
                    logger.info("Shutting down the publisher");
                    publisher.shutdown();
                    publisher.awaitTermination(1, TimeUnit.MINUTES);
                } catch (java.lang.InterruptedException ex) {
                    logger.error("Publisher termination error", ex);
                }
            }
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DoPub.class.getName());

    private PropertiesConfiguration config;
    
    private Publisher publisher;
}
