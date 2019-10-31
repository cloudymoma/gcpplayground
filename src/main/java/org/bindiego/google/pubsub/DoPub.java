package org.bindiego.google.pubsub;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.batching.BatchingSettings;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.FileInputStream;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.threeten.bp.Duration;

class DoPub implements Runnable {
    private DoPub() {}

    public DoPub(ProjectTopicName topicName, CredentialsProvider credentialsProvider){
        // Instantiate or get the current Global config
        config = Config.getConfig();

        try {
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

            // create the publisher
            publisher = Publisher.newBuilder(topicName)
                .setCredentialsProvider(credentialsProvider)
                .setBatchingSettings(batchingSettings)
                .setRetrySettings(retrySettings)
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

            for (int i = 0; i < numLoops; ++i) {
                final long millis = System.currentTimeMillis();
                final String message = 
                    new StringBuilder().append(millis).append(deli)
                        .append(threadId).append(deli)
                        .append(threadName).append(deli)
                        .append(i)
                        .toString();

                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                ApiFutures.addCallback(
                    messageIdFuture,
                    new ApiFutureCallback<String>() {
                        public void onFailure(Throwable throwable) {
                            logger.warn("failed to publish message: " + message);

                            if (throwable instanceof ApiException) {
                                ApiException apiException = ((ApiException) throwable);
                                logger.debug("Error code: " + apiException.getStatusCode().getCode());
                                logger.debug("Is retryable? " + apiException.isRetryable());
                            }
                        }
                        public void onSuccess(String messageId) {
                            logger.info("published with message id: " + messageId);
                        }
                    },
                    Executors.newSingleThreadExecutor());
            }
        } catch (Exception ex) {
            logger.error("Error", ex);
        } finally {
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
