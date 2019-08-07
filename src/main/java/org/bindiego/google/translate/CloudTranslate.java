package org.bindiego.google.translate;

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

import com.google.cloud.translate.Detection;
import com.google.cloud.translate.Translate;
// import com.google.cloud.translate.Translate.TranslateOption;
import com.google.cloud.translate.TranslateOptions;
// import com.google.cloud.translate.Translation;

public class CloudTranslate extends Thread {
    public CloudTranslate() {
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
        Translate trans = TranslateOptions.newBuilder()
            .setCredentials(credentials)
            .build()
            .getService();

        // text to translate
        final String txt = "A nação tem uma longa história, composta por diversos períodos distintos. A civilização chinesa clássica — uma das mais antigas do mundo — floresceu na bacia fértil do rio Amarelo, na planície norte do país. O sistema político chinês era baseado em monarquias hereditárias, conhecidas como dinastias, que tiveram seu início com a semimitológica Xia (aproximadamente 2 000 a.C.) e terminaram com a queda dos Qing, em 1911.";

        // Detect the language of the text
        Detection detection = trans.detect(txt);
        String detectedLanguage = detection.getLanguage();

        // Setup the threading pool
        int numThreads = Integer.parseInt(
            config.getProperty("google.translate.threads").toString());
        bq = new ArrayBlockingQueue<Runnable>(numThreads);
        exec = new ThreadPoolExecutor(numThreads, numThreads, 60, TimeUnit.SECONDS, bq);

        // Run threads
        for (int i = 0; i < numThreads; ++i) {
            exec.execute(new DoTranslate(trans, detectedLanguage, txt));
        }
    }

    private static final Logger logger =
        LogManager.getFormatterLogger(CloudTranslate.class.getName());

    private PropertiesConfiguration config;

    private ExecutorService exec;
    private BlockingQueue<Runnable> bq;
}
