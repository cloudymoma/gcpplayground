package org.bindiego;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.reloading.FileChangedReloadingStrategy;
import org.apache.commons.configuration.ConfigurationException;

//import org.bindiego.servicepal.FaceRecognition;
import org.bindiego.util.Config;

import org.bindiego.google.translate.CloudTranslate;
import org.bindiego.google.bq.CloudBigQuery;
import org.bindiego.google.pubsub.PubSub;
import org.bindiego.google.pubsub.lite.PubSubLite;
import org.bindiego.google.gcs.CloudStorage;

/**
 * ServicePal image identification application
 * http://www.faceplusplus.com/
 */
public class App
{
    private static final Logger logger =
        LogManager.getFormatterLogger(App.class.getName());

    private static PropertiesConfiguration config;

    public static void main(String... args)
    {
        config = Config.getConfig();

        logger.info(config.getProperty("app.name").toString() + " started");

        // new FaceRecognition();

        // Setup http proxy: https://github.com/bindiego/local_services/tree/develop/nginx/proxy
        if (config.getProperty("http.proxy").toString().equalsIgnoreCase("on")) {
            logger.info("Using http proxy");

            //System.setProperty("http.proxyHost", config.getProperty("http.proxy.host").toString());
            //System.setProperty("http.proxyPort", config.getProperty("http.proxy.port").toString());
            System.setProperty("https.proxyHost", config.getProperty("http.proxy.host").toString());
            System.setProperty("https.proxyPort", config.getProperty("http.proxy.port").toString());
        } else {
            System.clearProperty("http.proxyHost");
        }

        if (config.getProperty("google.translate").toString().equalsIgnoreCase("on"))
            new CloudTranslate().start();

        if (config.getProperty("google.bigquery").toString().equalsIgnoreCase("on"))
            new CloudBigQuery().start();

        if (config.getProperty("google.pubsub").toString().equalsIgnoreCase("on"))
            new PubSub().start();

        if (config.getProperty("google.pubsublite").toString().equalsIgnoreCase("on"))
            new PubSubLite().start();

        if (config.getProperty("google.gcs").toString().equalsIgnoreCase("on"))
            new CloudStorage().start();

        logger.info(config.getProperty("app.name").toString() + " Stopped");
    }
}
