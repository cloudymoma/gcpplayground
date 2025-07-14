package org.bindiego.util;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class GlobalTimer {

    private static GlobalTimer INSTANCE;
    private final AtomicBoolean printSwitch = new AtomicBoolean(true);
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
    private final long period;
    private final TimeUnit unit;

    private GlobalTimer(long period, TimeUnit unit) {
        this.period = period;
        this.unit = unit;
    }

    public static synchronized void init(long period, TimeUnit unit) {
        if (INSTANCE == null) {
            INSTANCE = new GlobalTimer(period, unit);
        } else {
            throw new IllegalStateException("GlobalTimer has already been initialized.");
        }
    }

    public static GlobalTimer getInstance() {
        if (INSTANCE == null) {
            throw new IllegalStateException("GlobalTimer has not been initialized. Call init() first.");
        }
        return INSTANCE;
    }

    public void start() {
        // Turn the switch on every configured period
        scheduler.scheduleAtFixedRate(() -> {
            printSwitch.set(true);
        }, 0, period, unit);
    }

    public boolean isSwitchOnAndFlip() {
        // Atomically check and set the switch to false
        return printSwitch.getAndSet(false);
    }

    public void shutdown() {
        scheduler.shutdown();
    }
}
