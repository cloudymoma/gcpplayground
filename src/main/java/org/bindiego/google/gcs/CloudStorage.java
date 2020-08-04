package org.bindiego.google.gcs;

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

import com.google.common.collect.Lists;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class CloudStorage extends Thread {
    public CloudStorage() {
        // Instantiate or get the current Global config
        config = Config.getConfig();
    }

    @Override
    public void run() {
        // Explicitly load google service account credentials
        String gCredentials = config.getProperty("google.credentials").toString();
        GoogleCredentials credentials = null;
        try {
            credentials = GoogleCredentials
                .fromStream(new FileInputStream(gCredentials))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        } catch (Exception ex) {
            logger.fatal("Credential loading failed", ex);
        }

        // Instantiate the Translation class
        Storage storage = StorageOptions.newBuilder()
            .setCredentials(credentials)
            .build()
            .getService();

        // Run threads
        int numThreads = Integer.parseInt(
            config.getProperty("google.gcs.threads").toString());
        exec = Executors.newFixedThreadPool(numThreads);

        for (int i = 0; i < numThreads; ++i) {
            exec.execute(new DoList(storage));
        }

        exec.shutdown();
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(CloudStorage.class.getName());

    private PropertiesConfiguration config;

    private ExecutorService exec;
    private BlockingQueue<Runnable> bq;
}
