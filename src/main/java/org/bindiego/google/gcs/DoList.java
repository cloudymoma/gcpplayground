package org.bindiego.google.gcs;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bindiego.util.Config;

import org.json.JSONObject;

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

class DoList implements Runnable {
    private DoList() {}

    public DoList(final Storage storage) {
        // Instantiate or get the current Global config
        config = Config.getConfig();

        this.storage = storage;
    }

    @Override
    public void run() {
        // loop control
        int numLoops = Integer.parseInt(
            config.getProperty("google.gcs.thread.requests").toString());

        for (int i = 0; i < numLoops; ++i) {
            try {
                Bucket bucket = this.storage.get(config.getProperty("google.gcs.bucket").toString());
                Page<Blob> blobs = bucket.list();

                for (Blob blob : blobs.iterateAll()) {
                    logger.info("GCS Object: %s", blob.getName());
                }
            } catch (Exception ex) {
                logger.error("BOOM!", ex);
                System.exit(-2);
            }
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(DoList.class.getName());

    private PropertiesConfiguration config;

    private Storage storage;
}
