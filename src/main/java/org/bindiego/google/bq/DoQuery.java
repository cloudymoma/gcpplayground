package org.bindiego.google.bq;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryResponse;
import com.google.cloud.bigquery.TableResult;
import java.util.UUID;

class DoQuery implements Runnable {
    private DoQuery() {}

    public DoQuery(final BigQuery bigquery) {
        // Instantiate or get the current Global config
        config = Config.getConfig();

        this.bigquery = bigquery;
    }

    @Override
    public void run() {
        // loop control
        int numLoops = Integer.parseInt(
            config.getProperty("google.bigquery.thread.requests").toString());

        for (int i = 0; i < numLoops; ++i) {
            try {
                // final String qStr = "select * from click_stream.event";
                final String qStr = "insert into click_stream.event"
                    + " (uid, event) "
                    + " values "
                    + " ('002', [STRUCT('k1', STRUCT('v1', 1, 1.2)),STRUCT('k2', STRUCT('v2', 12, 1.23)),STRUCT('k3', STRUCT('v3', 123, 1.234))])";

                QueryJobConfiguration queryConf = 
                    QueryJobConfiguration.newBuilder(
                        qStr)
                    .setUseLegacySql(false)
                    .build();

                // Create a job ID so that we can safely retry.
                JobId jobId = JobId.of(UUID.randomUUID().toString());
                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConf).setJobId(jobId).build());

                // Wait for the query to complete.
                queryJob = queryJob.waitFor();

                // Check for errors
                if (queryJob == null) {
                      throw new RuntimeException("Job no longer exists");
                } else if (queryJob.getStatus().getError() != null) {
                    // You can also look at queryJob.getStatus().getExecutionErrors() for all
                    // errors, not just the latest one.
                    throw new RuntimeException(queryJob.getStatus().getError().toString());
                }

                TableResult result = queryJob.getQueryResults();

                for (FieldValueList row : result.iterateAll()) {
                    // String url = row.get("url").getStringValue();
                    // long viewCount = row.get("view_count").getLongValue();
                    // System.out.printf("url: %s views: %d%n", url, viewCount);
                    //System.out.println(row);
                    for (FieldValue val : row) {
                        System.out.printf("%s,", val.toString());
                    }
                    System.out.printf("\n");
                }
            } catch (Exception ex) {
                logger.error("BOOM!", ex);
                System.exit(-2);
            }
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DoQuery.class.getName());

    private PropertiesConfiguration config;

    private BigQuery bigquery;
}
