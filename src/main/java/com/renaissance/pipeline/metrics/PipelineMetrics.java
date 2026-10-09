package com.renaissance.pipeline.metrics;

import com.renaissance.pipeline.metrics.MetricsSnapshot.StageStats;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Потокобезопасные счётчики и резервуары сэмплов глубины/латентности пайплайна.
 */
public final class PipelineMetrics {

    private final AtomicInteger inFlight = new AtomicInteger(0);
    private final AtomicLong arrived = new AtomicLong(0);
    private final AtomicLong departed = new AtomicLong(0);

    private final Object lock = new Object();
    private final List<Integer> inDepthSamples = new ArrayList<>();
    private final List<Integer> poolInFlightSamples = new ArrayList<>();
    private final List<Integer> outDepthSamples = new ArrayList<>();
    private final List<Long> poolLatencyMsSamples = new ArrayList<>();
    private final List<Double> inRateSamples = new ArrayList<>();
    private final List<Double> outRateSamples = new ArrayList<>();

    private volatile long startedAtMs = -1L;
    private volatile long stoppedAtMs = -1L;

    /** Зафиксировать время старта сэмплирования. */
    public void markStart() {
        startedAtMs = System.currentTimeMillis();
    }

    /** Зафиксировать время остановки сэмплирования. */
    public void markStop() {
        stoppedAtMs = System.currentTimeMillis();
    }

    /** Сэмпл глубины входной очереди. */
    public void recordInDepth(int depth) {
        synchronized (lock) {
            inDepthSamples.add(depth);
        }
    }

    /** Сэмпл числа in-flight worker'ов. */
    public void recordPoolInFlight(int inFlightCount) {
        synchronized (lock) {
            poolInFlightSamples.add(inFlightCount);
        }
    }

    /** Сэмпл глубины reorder/Out backlog. */
    public void recordOutDepth(int depth) {
        synchronized (lock) {
            outDepthSamples.add(depth);
        }
    }

    /** Сообщение назначено Reader'ом (arrived++). */
    public void recordArrived() {
        arrived.incrementAndGet();
    }

    /** Сообщение записано Writer'ом (departed++). */
    public void recordDeparted() {
        departed.incrementAndGet();
    }

    /** Сэмпл латентности обработки в пуле, мс. */
    public void recordPoolLatencyMs(long latencyMs) {
        synchronized (lock) {
            poolLatencyMsSamples.add(Math.max(0L, latencyMs));
        }
    }

    /** Сэмпл мгновенной скорости In (msg/s за интервал сэмпла). */
    public void recordInRate(double msgPerSec) {
        synchronized (lock) {
            inRateSamples.add(Math.max(0.0, msgPerSec));
        }
    }

    /** Сэмпл мгновенной скорости Out (msg/s за интервал сэмпла). */
    public void recordOutRate(double msgPerSec) {
        synchronized (lock) {
            outRateSamples.add(Math.max(0.0, msgPerSec));
        }
    }

    /** Увеличить in-flight после take не-poison сообщения. */
    public void onWorkerTake() {
        inFlight.incrementAndGet();
    }

    /** Уменьшить in-flight и записать латентность после put/tombstone. */
    public void onWorkerComplete(long latencyMs) {
        inFlight.decrementAndGet();
        recordPoolLatencyMs(latencyMs);
    }

    /** Текущее число сообщений в обработке у worker'ов. */
    public int inFlight() {
        return inFlight.get();
    }

    /** Число arrived. */
    public long arrivedCount() {
        return arrived.get();
    }

    /** Число departed. */
    public long departedCount() {
        return departed.get();
    }

    /** Неизменяемый снимок статистик и скоростей. */
    public MetricsSnapshot snapshot() {
        List<Integer> inCopy;
        List<Integer> poolCopy;
        List<Integer> outCopy;
        List<Long> latencyCopy;
        List<Double> inRateCopy;
        List<Double> outRateCopy;
        synchronized (lock) {
            inCopy = new ArrayList<>(inDepthSamples);
            poolCopy = new ArrayList<>(poolInFlightSamples);
            outCopy = new ArrayList<>(outDepthSamples);
            latencyCopy = new ArrayList<>(poolLatencyMsSamples);
            inRateCopy = new ArrayList<>(inRateSamples);
            outRateCopy = new ArrayList<>(outRateSamples);
        }

        long start = startedAtMs;
        long stop = stoppedAtMs > 0 ? stoppedAtMs : System.currentTimeMillis();
        long durationMs = start > 0 ? Math.max(0L, stop - start) : 0L;
        double seconds = durationMs / 1000.0;
        long arr = arrived.get();
        long dep = departed.get();
        double inRateOverall = seconds > 0 ? arr / seconds : 0.0;
        double outRateOverall = seconds > 0 ? dep / seconds : 0.0;

        return new MetricsSnapshot(
                StageStats.of(inCopy),
                StageStats.of(poolCopy),
                StageStats.of(outCopy),
                StageStats.of(inRateCopy),
                StageStats.of(outRateCopy),
                inRateOverall,
                outRateOverall,
                durationMs,
                StageStats.of(latencyCopy),
                arr,
                dep,
                inCopy.size());
    }
}
