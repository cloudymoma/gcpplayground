package org.bindiego.google.pubsub.lite;

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
import com.google.api.gax.core.FixedExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.api.gax.retrying.RetrySettings;
import com.google.auth.oauth2.ServiceAccountCredentials;
import io.grpc.StatusException;
// import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Timestamp.Builder;
import java.io.FileInputStream;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Random;
import java.util.List;
import java.util.ArrayList;
import org.threeten.bp.Duration;

import com.google.pubsub.v1.PubsubMessage;
import com.google.cloud.pubsublite.*;
import com.google.cloud.pubsublite.cloudpubsub.*;

class DoPubLite implements Runnable {
    private DoPubLite() {}

    public DoPubLite(TopicPath topicPath, CredentialsProvider credentialsProvider){
        // Instantiate or get the current Global config
        config = Config.getConfig();

        try {
            PublisherSettings publisherSettings = 
                PublisherSettings.newBuilder().setTopicPath(topicPath).build();

            this.publisher = Publisher.create(publisherSettings);
        } catch (Exception ex) {
            logger.error("Failed to init publisher", ex);
        }
    }

    @Override
    public void run() {
        List<ApiFuture<String>> futures = new ArrayList<>();

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

            Random rand = new Random();

            // a random dimension array
            String[] dims = {"bindigo", 
                "bindiego",
                "ivy",
                "duelaylowmow"};

            /**
             * CSV payload contents
             * - event timestamp (milliseconds)
             * - thread_id
             * - thread_name
             * - sequence_num (how many messages posted by this thread, monotonic increasing
             * - dim1
             * - metrics1
             */
            for (int i = 0; i < numLoops; ++i) {
                // introduce a random delay in 5s, 10s and 30s for event time
                final long[] delay = {5000L, 10000L, 30000L};
                final long millis = (0 == rand.nextInt(2)) ? 
                    System.currentTimeMillis() :
                    (System.currentTimeMillis() - delay[rand.nextInt(3)]);

                final String message = 
                    new StringBuilder().append(millis).append(deli)
                        .append(threadId).append(deli)
                        .append(threadName).append(deli)
                        .append(i).append(deli)
                        .append(dims[rand.nextInt(4)]).append(deli)
                        .append(rand.nextInt(1000))
                        .toString();

                final String msgId = UUID.randomUUID().toString();

                ByteString data = ByteString.copyFromUtf8(message);

                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Publish a message. Messages are automatically batched.
                ApiFuture<String> future = publisher.publish(pubsubMessage);
                futures.add(future);
            }
        } catch (Exception ex) {
            logger.error("Error", ex);
        } finally {
            try {
                ArrayList<PublishMetadata> metadata = new ArrayList<>();
                List<String> ackIds = ApiFutures.allAsList(futures).get();

                for (String id : ackIds) {
                    // Decoded metadata contains partition and offset.
                    metadata.add(PublishMetadata.decode(id));
                }

                logger.info("%s\nPublished %d messages", metadata, ackIds.size());

                if (publisher != null) {
                    // Shut down the publisher.
                    publisher.stopAsync().awaitTerminated();
                    logger.info("Publisher is shut down.");
                }
            } catch (InterruptedException ex) {
                logger.error("Error finalizing Publisher", ex);
            } catch (StatusException ex) {
                logger.error("gRPC Error", ex);
            } catch (Exception ex) {
                logger.error("Error", ex);
            }
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DoPubLite.class.getName());

    private PropertiesConfiguration config;
    
    private Publisher publisher;

    private ExecutorService executor = Executors.newCachedThreadPool();
}
