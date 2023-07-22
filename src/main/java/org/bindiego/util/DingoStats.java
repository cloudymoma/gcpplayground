package org.bindiego.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.lang.Math;
import java.util.concurrent.atomic.AtomicInteger;
import java.math.BigDecimal;

import org.bindiego.Settings;

public class DingoStats implements Cloneable {

    private static final Logger logger =
        LogManager.getFormatterLogger(DingoStats.class.getName());

    public DingoStats clone() throws CloneNotSupportedException {
        throw new CloneNotSupportedException();
    }

    private String statsName;

    private AtomicInteger counter;

    private volatile double min;
    private volatile double max;

    // only store running mean, sum of squared deviations (m2) and count
    // Incrementally update mean and m2 with each new number
    // m2 tracks the sum of squared deviations from the true mean
    // This avoids accumulating the deviations themselves
    // Standard deviation can be calculated at any point as sqrt(m2 / (n - 1))
    private volatile double mean;
    private volatile double stdDev;
    private volatile double m2;


    public DingoStats(String statsName) {
        this.statsName = statsName;

        counter = new AtomicInteger(0);
        min = Double.MAX_VALUE;
        max = Double.MIN_VALUE;
        mean = 0.0d;
        stdDev = 0.0d;
        m2 = 0.0d;
    }

    // Streaming calculation for unbounded incoming numbers without holding
    // all numbers or running sum for potential memory leaks
    public synchronized void add(double x) {
        int n = counter.incrementAndGet();

        min = Math.min(min, x);
        max = Math.max(max, x);

        double delta = x - mean;
        mean += delta / n;
        m2 += delta * (x - mean - delta / n);
        // stdDev = Math.sqrt(m2 / (n - 1));
    }

    public void show() {
        logger.info("============" + statsName + " stats =============");
        logger.info("Count: " + counter);
        logger.info("Min: " + min);
        logger.info("Max: " + max);
        logger.info("Average: " + mean);
        logger.info("Standard Deviation: " + Math.sqrt(m2 / (counter.get() - 1)));
    }
}
