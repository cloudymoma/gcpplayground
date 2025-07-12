package org.bindiego.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.Math;

public class DingoStats {

    private static final Logger logger =
        LogManager.getFormatterLogger(DingoStats.class.getName());

    private String statsName;

    private long counter;

    private double min;
    private double max;

    // only store running mean, sum of squared deviations (m2) and count
    // Incrementally update mean and m2 with each new number
    // m2 tracks the sum of squared deviations from the true mean
    // This avoids accumulating the deviations themselves
    // Standard deviation can be calculated at any point as sqrt(m2 / (n - 1))
    private double mean;
    private double m2;

    public DingoStats(String statsName) {
        this.statsName = statsName;

        counter = 0L;
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        mean = 0.0d;
        m2 = 0.0d;
    }

    // Streaming calculation for unbounded incoming numbers without holding
    // all numbers or running sum for potential memory leaks
    public synchronized void add(double x) {
        counter++;
        long n = counter;
        min = Math.min(min, x);
        max = Math.max(max, x);

        double delta = x - mean;
        mean += delta / n;
        double delta2 = x - mean;
        m2 += delta * delta2;
    }

    public synchronized void show() {
        logger.info("============" + statsName + " stats =============");
        long n = counter;
        logger.info("Count: " + n);
        if (n == 0) {
            logger.info("Min: N/A");
            logger.info("Max: N/A");
            logger.info("Average: N/A");
            logger.info("Standard Deviation: N/A");
            return;
        }
        logger.info("Min: " + min);
        logger.info("Max: " + max);
        logger.info("Average: " + mean);
        if (n > 1) {
            logger.info("Standard Deviation: " + Math.sqrt(m2 / (n - 1)));
        } else {
            logger.info("Standard Deviation: 0.0");
        }
    }
}
