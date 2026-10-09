package com.renaissance.pipeline.config;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AppConfigTest {

    @Test
    void defaultsWhenPropertiesAbsentOrEmpty() throws Exception {
        AppConfig fromEmpty = AppConfig.load(new ByteArrayInputStream(new byte[0]));
        assertDefaults(fromEmpty);

        AppConfig fromMissingClasspath = AppConfig.loadFromClasspath("definitely-missing-config-xyz.properties");
        assertDefaults(fromMissingClasspath);
    }

    @Test
    void allKeysReadFromFileDifferFromDefaults() throws Exception {
        String content = String.join("\n",
                "app.t1.ms=10",
                "app.t2.ms=40",
                "app.in.dir=CustomIn",
                "app.out.dir=CustomOut",
                "app.queue.capacity=7",
                "app.reorder.window=32",
                "app.workers=5",
                "app.write.retry.count=1",
                "app.write.retry.delay.ms=9",
                "app.log.file=logs/custom-pipeline.log",
                "app.metrics.enabled=false",
                "app.metrics.sample.ms=100");
        AppConfig cfg = AppConfig.load(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));

        assertEquals(10L, cfg.t1Ms());
        assertEquals(40L, cfg.t2Ms());
        assertTrue(cfg.inDir().endsWith("CustomIn"));
        assertTrue(cfg.outDir().endsWith("CustomOut"));
        assertEquals(7, cfg.queueCapacity());
        assertEquals(32, cfg.reorderWindow());
        assertEquals(5, cfg.workersConfigured());
        assertEquals(5, cfg.workersEffective());
        assertEquals(1, cfg.writeRetryCount());
        assertEquals(9L, cfg.writeRetryDelayMs());
        assertLogFileEndsWithTimestampPrefix(cfg, "custom-pipeline.log");
        assertEquals(false, cfg.metricsEnabled());
        assertEquals(100L, cfg.metricsSampleMs());
    }

    @Test
    void failWhenPositiveKeysAreZeroOrNegative() {
        assertFails("app.t1.ms=0");
        assertFails("app.t1.ms=-1");
        assertFails("app.t2.ms=0");
        assertFails("app.queue.capacity=0");
        assertFails("app.reorder.window=-5");
    }

    @Test
    void failWhenPositiveKeysAreNonNumeric() {
        assertFails("app.t1.ms=abc");
        assertFails("app.t2.ms=12x");
        assertFails("app.queue.capacity=?");
        assertFails("app.reorder.window=one");
    }

    @Test
    void failWhenWorkersNegative() {
        assertFails("app.workers=-1");
    }

    @Test
    void workersZeroComputesCeilT2OverT1() throws Exception {
        AppConfig a = loadProps(
                "app.t1.ms=100",
                "app.t2.ms=300",
                "app.workers=0");
        assertEquals(0, a.workersConfigured());
        assertEquals(3, a.workersEffective());

        AppConfig b = loadProps(
                "app.t1.ms=100",
                "app.t2.ms=250",
                "app.workers=0");
        assertEquals(3, b.workersEffective());
    }

    @Test
    void workersPositiveUsesConfiguredValue() throws Exception {
        AppConfig cfg = loadProps(
                "app.t1.ms=100",
                "app.t2.ms=300",
                "app.workers=7");
        assertEquals(7, cfg.workersConfigured());
        assertEquals(7, cfg.workersEffective());
    }

    @Test
    void t2LessOrEqualT1DoesNotFail() throws Exception {
        AppConfig cfg = loadProps(
                "app.t1.ms=100",
                "app.t2.ms=50",
                "app.workers=0");
        assertEquals(100L, cfg.t1Ms());
        assertEquals(50L, cfg.t2Ms());
        assertEquals(1, cfg.workersEffective());
    }

    @Test
    void loadFromClasspathTestProperties() {
        AppConfig cfg = AppConfig.loadFromClasspath("application-test.properties");
        assertEquals(1L, cfg.t1Ms());
        assertEquals(3L, cfg.t2Ms());
        assertEquals(10, cfg.queueCapacity());
        assertEquals(16, cfg.reorderWindow());
        assertEquals(0, cfg.workersConfigured());
        assertEquals(3, cfg.workersEffective());
    }

    private static void assertDefaults(AppConfig cfg) {
        assertEquals(AppConfig.DEFAULT_T1_MS, cfg.t1Ms());
        assertEquals(AppConfig.DEFAULT_T2_MS, cfg.t2Ms());
        assertEquals(AppConfig.DEFAULT_QUEUE_CAPACITY, cfg.queueCapacity());
        assertEquals(AppConfig.DEFAULT_REORDER_WINDOW, cfg.reorderWindow());
        assertEquals(AppConfig.DEFAULT_WORKERS, cfg.workersConfigured());
        assertEquals(3, cfg.workersEffective());
        assertEquals(AppConfig.DEFAULT_WRITE_RETRY_COUNT, cfg.writeRetryCount());
        assertEquals(AppConfig.DEFAULT_WRITE_RETRY_DELAY_MS, cfg.writeRetryDelayMs());
        assertEquals(AppConfig.DEFAULT_METRICS_ENABLED, cfg.metricsEnabled());
        assertEquals(AppConfig.DEFAULT_METRICS_SAMPLE_MS, cfg.metricsSampleMs());
        assertTrue(cfg.inDir().endsWith("In"));
        assertTrue(cfg.outDir().endsWith("Out"));
        assertLogFileEndsWithTimestampPrefix(cfg, "pipeline.log");
    }

    /** Ожидаем {@code .../logs/YYYYMMDDHHMMSS_<baseName>}. */
    private static void assertLogFileEndsWithTimestampPrefix(AppConfig cfg, String baseName) {
        String path = cfg.logFile().toString().replace('\\', '/');
        assertTrue(path.matches(".*/logs/\\d{14}_" + java.util.regex.Pattern.quote(baseName)),
                "unexpected log file path: " + path);
    }

    private static AppConfig loadProps(String... lines) throws Exception {
        String content = String.join("\n", lines);
        return AppConfig.load(new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8)));
    }

    private static void assertFails(String line) {
        Properties props = new Properties();
        String[] parts = line.split("=", 2);
        props.setProperty(parts[0], parts[1]);
        // keep other required positives at defaults via empty missing keys
        IllegalStateException ex = assertThrows(IllegalStateException.class, () -> AppConfig.fromProperties(props));
        assertTrue(ex.getMessage().contains(parts[0]) || ex.getMessage().toLowerCase().contains("invalid"),
                () -> "unexpected message: " + ex.getMessage());
    }
}
