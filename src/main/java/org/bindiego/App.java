package org.bindiego;

import org.apache.commons.configuration.PropertiesConfiguration;

//import org.bindiego.servicepal.FaceRecognition;
import org.bindiego.util.Config;
import org.bindiego.util.GlobalTimer;

import org.bindiego.google.translate.CloudTranslate;
import org.bindiego.google.bq.CloudBigQuery;
import org.bindiego.google.pubsub.PubSub;
import org.bindiego.google.pubsub.lite.PubSubLite;
import org.bindiego.google.gcs.CloudStorage;
import org.bindiego.google.kafka.DingoKafka;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        final List<Thread> services = new ArrayList<>();

        config = Config.getConfig();

        GlobalTimer.init(5, TimeUnit.SECONDS);
        GlobalTimer.getInstance().start();

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

        if (config.getProperty("google.translate").toString().equalsIgnoreCase("on")) {
            Thread service = new CloudTranslate();
            services.add(service);
            service.start();
        }

        if (config.getProperty("google.bigquery").toString().equalsIgnoreCase("on")) {
            Thread service = new CloudBigQuery();
            services.add(service);
            service.start();
        }

        if (config.getProperty("google.pubsub").toString().equalsIgnoreCase("on")) {
            Thread service = new PubSub();
            services.add(service);
            service.start();
        }

        if (config.getProperty("google.pubsublite").toString().equalsIgnoreCase("on")) {
            Thread service = new PubSubLite();
            services.add(service);
            service.start();
        }

        if (config.getProperty("google.gcs").toString().equalsIgnoreCase("on")) {
            Thread service = new CloudStorage();
            services.add(service);
            service.start();
        }

        if (config.getProperty("google.kafka").toString().equalsIgnoreCase("on")) {
            Thread service = new DingoKafka();
            services.add(service);
            service.start();
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook initiated, stopping services...");
            /*
            for (Thread service : services) {
                if (service.isAlive()) {
                    logger.info("Stopping service: " + service.getName());
                    service.interrupt();
                }
            }
            */
            for (Thread service : services) {
                try {
                    // Wait up to 3 seconds for the service to terminate after interruption.
                    service.join(3000);
                } catch (InterruptedException e) {
                    logger.error("Shutdown hook interrupted while waiting for service " + service.getName() + " to stop.", e);
                    // Preserve the interrupted status
                    Thread.currentThread().interrupt();
                }
                if (service.isAlive()) {
                    logger.warn("Service {} did not terminate within 3 seconds.", service.getName());
                }
            }
            logger.info("All services have been signaled to stop. Application will now exit.");
        }));

        logger.info(config.getProperty("app.name").toString() + " is running. Press Ctrl+C to exit.");
    }
}
