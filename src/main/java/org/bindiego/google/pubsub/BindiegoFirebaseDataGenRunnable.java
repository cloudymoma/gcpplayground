package org.bindiego.google.pubsub;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import org.bindiego.util.BindiegoFirebaseDataGen;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * A Runnable class that acts as a data producer.
 * It continuously generates Firebase event data and adds it to a thread-safe
 * BlockingQueue, applying exponential backoff if the queue is full.
 */
public class BindiegoFirebaseDataGenRunnable implements Runnable {

    // The shared, thread-safe collection to hold generated data.
    private final BlockingQueue<String> dataQueue;
    
    // An instance of our data generator.
    private final BindiegoFirebaseDataGen generator;

    // Constants for the exponential backoff mechanism.
    private static final long INITIAL_BACKOFF_MS = 100; // 100 milliseconds
    private static final long MAX_BACKOFF_MS = 10_000;    // 10 seconds

    /**
     * Constructs a new data generator runnable.
     * @param dataQueue A thread-safe BlockingQueue instance where generated data will be stored.
     */
    public BindiegoFirebaseDataGenRunnable(BlockingQueue<String> dataQueue) {
        this.dataQueue = dataQueue;
        this.generator = new BindiegoFirebaseDataGen();
    }

    @Override
    public void run() {
        logger.info("✅ Data generator thread started. Populating queue...");
        long currentBackoff = INITIAL_BACKOFF_MS;

        try {
            // Loop indefinitely until the thread is interrupted.
            while (!Thread.currentThread().isInterrupted()) {
                String newEvent = generator.generateRandomEvent();

                // offer() is non-blocking and returns false immediately if the queue is full.
                if (dataQueue.offer(newEvent)) {
                    // Success! Reset backoff delay for the next cycle.
                    currentBackoff = INITIAL_BACKOFF_MS;
                } else {
                    // The queue is full. Time to back off.
                    // logger.error("⚠️ Queue is full. Backing off for %d ms.%n", currentBackoff);
                    
                    // Wait for the calculated backoff period.
                    Thread.sleep(currentBackoff);
                    
                    // Double the backoff for the next potential failure, up to the max limit.
                    currentBackoff = Math.min(currentBackoff * 2, MAX_BACKOFF_MS);
                }
            }
        } catch (InterruptedException e) {
            // Gracefully exit if the thread is interrupted (e.g., during Thread.sleep).
            Thread.currentThread().interrupt(); // Preserve the interrupted status.
            logger.info("Data generator thread interrupted. Shutting down.");
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(
            BindiegoFirebaseDataGenRunnable.class.getName());
}