package com.renaissance.pipeline.metrics;

import com.renaissance.pipeline.util.SimpleLog;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntSupplier;

/**
 * Daemon-сэмплер: каждые {@code samplePeriodMs} пишет глубины In / ThreadPool / Out.
 */
public final class MetricsSampler implements AutoCloseable {

    private final PipelineMetrics metrics;
    private final IntSupplier inDepth;
    private final IntSupplier poolInFlight;
    private final IntSupplier outDepth;
    private final long samplePeriodMs;
    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AtomicBoolean stopped = new AtomicBoolean(false);
    private long lastSampleAtMs = -1L;
    private long lastArrived;
    private long lastDeparted;

    /** Сэмплер глубин очереди, in-flight worker'ов и reorder-буфера. */
    public MetricsSampler(
            PipelineMetrics metrics,
            IntSupplier inDepth,
            IntSupplier poolInFlight,
            IntSupplier outDepth,
            long samplePeriodMs) {
        this.metrics = Objects.requireNonNull(metrics, "metrics");
        this.inDepth = Objects.requireNonNull(inDepth, "inDepth");
        this.poolInFlight = Objects.requireNonNull(poolInFlight, "poolInFlight");
        this.outDepth = Objects.requireNonNull(outDepth, "outDepth");
        if (samplePeriodMs <= 0) {
            throw new IllegalArgumentException("samplePeriodMs must be > 0, got " + samplePeriodMs);
        }
        this.samplePeriodMs = samplePeriodMs;
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "metrics-sampler");
            t.setDaemon(true);
            return t;
        });
    }

    /** Запуск периодического сэмплирования. */
    public void start() {
        if (!started.compareAndSet(false, true)) {
            return;
        }
        metrics.markStart();
        scheduler.scheduleAtFixedRate(this::sampleSafely, 0L, samplePeriodMs, TimeUnit.MILLISECONDS);
        SimpleLog.log("MetricsSampler started: periodMs=" + samplePeriodMs);
    }

    /** Остановка scheduler и фиксация времени stop в метриках. */
    public void stop() {
        if (!stopped.compareAndSet(false, true)) {
            return;
        }
        metrics.markStop();
        scheduler.shutdownNow();
        try {
            scheduler.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** То же, что {@link #stop()}. */
    @Override
    public void close() {
        stop();
    }

    /** Один тик сэмпла; ошибки не роняют scheduler. */
    private void sampleSafely() {
        try {
            metrics.recordInDepth(Math.max(0, inDepth.getAsInt()));
            metrics.recordPoolInFlight(Math.max(0, poolInFlight.getAsInt()));
            metrics.recordOutDepth(Math.max(0, outDepth.getAsInt()));

            long now = System.currentTimeMillis();
            long arr = metrics.arrivedCount();
            long dep = metrics.departedCount();
            if (lastSampleAtMs > 0) {
                double dtSec = (now - lastSampleAtMs) / 1000.0;
                if (dtSec > 0) {
                    metrics.recordInRate((arr - lastArrived) / dtSec);
                    metrics.recordOutRate((dep - lastDeparted) / dtSec);
                }
            }
            lastSampleAtMs = now;
            lastArrived = arr;
            lastDeparted = dep;
        } catch (RuntimeException e) {
            SimpleLog.log("MetricsSampler sample failed: " + e.getMessage());
        }
    }
}
