package org.bindiego.google.bq;

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

import com.google.common.collect.Lists;

import com.google.auth.oauth2.GoogleCredentials;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;

public class CloudBigQuery extends Thread {
    public CloudBigQuery () {
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

        // Instantiate the BigQuery class
        BigQuery bigquery = BigQueryOptions.newBuilder()
            .setCredentials(credentials)
            .build()
            .getService();

        // Setup the threading pool
        bq = new ArrayBlockingQueue<Runnable>(10);
        exec = new ThreadPoolExecutor(2, 10, 60, TimeUnit.SECONDS, bq);

        // Run threads
        int numThreads = Integer.parseInt(
            config.getProperty("google.bigquery.threads").toString());
        for (int i = 0; i < numThreads; ++i) {
            exec.execute(new DoQuery(bigquery));
        }

        exec.shutdown();
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(BigQuery.class.getName());

    private PropertiesConfiguration config;

    private ExecutorService exec;
    private BlockingQueue<Runnable> bq;
}
