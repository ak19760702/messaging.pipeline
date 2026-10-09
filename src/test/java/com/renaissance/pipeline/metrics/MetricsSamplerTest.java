package com.renaissance.pipeline.metrics;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class MetricsSamplerTest {

    @Test
    void samplesDepthsUntilStopped() throws Exception {
        PipelineMetrics metrics = new PipelineMetrics();
        AtomicInteger in = new AtomicInteger(3);
        AtomicInteger pool = new AtomicInteger(2);
        AtomicInteger out = new AtomicInteger(1);

        try (MetricsSampler sampler = new MetricsSampler(
                metrics, in::get, pool::get, out::get, 20L)) {
            sampler.start();
            Thread.sleep(120);
        }

        MetricsSnapshot snap = metrics.snapshot();
        assertTrue(snap.sampleCount() >= 2, "expected several samples, got " + snap.sampleCount());
        assertTrue(snap.in().avg() >= 2.5, "in avg=" + snap.in().avg());
        assertTrue(snap.threadPool().max() >= 2.0);
        assertTrue(snap.out().p99() >= 1.0);
    }

    @Test
    void snapshotIncludesRatesAndLatency() {
        PipelineMetrics metrics = new PipelineMetrics();
        metrics.markStart();
        metrics.recordArrived();
        metrics.recordArrived();
        metrics.recordDeparted();
        metrics.onWorkerTake();
        metrics.onWorkerComplete(10);
        metrics.onWorkerTake();
        metrics.onWorkerComplete(30);
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        metrics.markStop();

        MetricsSnapshot snap = metrics.snapshot();
        assertTrue(snap.arrived() == 2);
        assertTrue(snap.departed() == 1);
        assertTrue(snap.durationMs() >= 40);
        assertTrue(snap.inRateMsgPerSec() > 0);
        assertTrue(snap.poolLatencyMs().max() >= 30);
        String table = snap.formatTable();
        assertTrue(table.contains("--- metrics ---"));
        assertTrue(table.contains("ThreadPool"));
        assertTrue(table.contains("msg"));
        assertTrue(table.contains("msg/s"));
        assertTrue(table.contains("pool latency"));
        assertTrue(table.contains("unit"));
    }
}
