package com.renaissance.pipeline.config;

import com.renaissance.pipeline.util.SimpleLog;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Properties;

/**
 * Загрузка и валидация application.properties (архитектура §5 / §7.1).
 */
public final class AppConfig {

    public static final String DEFAULT_RESOURCE = "application.properties";

    public static final long DEFAULT_T1_MS = 100L;
    public static final long DEFAULT_T2_MS = 300L;
    public static final String DEFAULT_IN_DIR = "In";
    public static final String DEFAULT_OUT_DIR = "Out";
    public static final int DEFAULT_QUEUE_CAPACITY = 100;
    public static final int DEFAULT_REORDER_WINDOW = 1000;
    public static final int DEFAULT_WORKERS = 0;
    public static final int DEFAULT_WRITE_RETRY_COUNT = 3;
    public static final long DEFAULT_WRITE_RETRY_DELAY_MS = 50L;
    public static final String DEFAULT_LOG_FILE = SimpleLog.DEFAULT_LOG_FILE;
    public static final boolean DEFAULT_METRICS_ENABLED = true;
    public static final long DEFAULT_METRICS_SAMPLE_MS = 50L;

    private final long t1Ms;
    private final long t2Ms;
    private final Path inDir;
    private final Path outDir;
    private final int queueCapacity;
    private final int reorderWindow;
    private final int workersConfigured;
    private final int workersEffective;
    private final int writeRetryCount;
    private final long writeRetryDelayMs;
    private final Path logFile;
    private final boolean metricsEnabled;
    private final long metricsSampleMs;

    /** Удобный ctor: метрики по умолчанию. */
    public AppConfig(
            long t1Ms,
            long t2Ms,
            Path inDir,
            Path outDir,
            int queueCapacity,
            int reorderWindow,
            int workersConfigured,
            int workersEffective,
            int writeRetryCount,
            long writeRetryDelayMs,
            Path logFile) {
        this(
                t1Ms, t2Ms, inDir, outDir, queueCapacity, reorderWindow,
                workersConfigured, workersEffective, writeRetryCount, writeRetryDelayMs,
                logFile, DEFAULT_METRICS_ENABLED, DEFAULT_METRICS_SAMPLE_MS);
    }

    /** Полный конструктор всех полей конфига. */
    public AppConfig(
            long t1Ms,
            long t2Ms,
            Path inDir,
            Path outDir,
            int queueCapacity,
            int reorderWindow,
            int workersConfigured,
            int workersEffective,
            int writeRetryCount,
            long writeRetryDelayMs,
            Path logFile,
            boolean metricsEnabled,
            long metricsSampleMs) {
        this.t1Ms = t1Ms;
        this.t2Ms = t2Ms;
        this.inDir = inDir;
        this.outDir = outDir;
        this.queueCapacity = queueCapacity;
        this.reorderWindow = reorderWindow;
        this.workersConfigured = workersConfigured;
        this.workersEffective = workersEffective;
        this.writeRetryCount = writeRetryCount;
        this.writeRetryDelayMs = writeRetryDelayMs;
        this.logFile = logFile;
        this.metricsEnabled = metricsEnabled;
        this.metricsSampleMs = metricsSampleMs;
    }

    /** Пауза Reader/Writer (t1), мс. */
    public long t1Ms() {
        return t1Ms;
    }

    /** Длительность обработки worker (t2), мс. */
    public long t2Ms() {
        return t2Ms;
    }

    /** Каталог In/. */
    public Path inDir() {
        return inDir;
    }

    /** Каталог Out/. */
    public Path outDir() {
        return outDir;
    }

    /** Ёмкость очереди Reader→Workers. */
    public int queueCapacity() {
        return queueCapacity;
    }

    /** Размер окна ReorderBuffer. */
    public int reorderWindow() {
        return reorderWindow;
    }

    /** Значение app.workers из конфига (0 = авто). */
    public int workersConfigured() {
        return workersConfigured;
    }

    /** Фактическое число worker-потоков. */
    public int workersEffective() {
        return workersEffective;
    }

    /** Число повторных попыток записи. */
    public int writeRetryCount() {
        return writeRetryCount;
    }

    /** Задержка между ретраями записи, мс. */
    public long writeRetryDelayMs() {
        return writeRetryDelayMs;
    }

    /** Путь к файлу лога. */
    public Path logFile() {
        return logFile;
    }

    /** Включены ли метрики. */
    public boolean metricsEnabled() {
        return metricsEnabled;
    }

    /** Период сэмплирования метрик, мс. */
    public long metricsSampleMs() {
        return metricsSampleMs;
    }

    /** Загрузка с classpath {@code application.properties} или defaults. */
    public static AppConfig load() {
        return loadFromClasspath(DEFAULT_RESOURCE);
    }

    /** Загрузка именованного ресурса с classpath; нет ресурса → defaults + warn. */
    public static AppConfig loadFromClasspath(String name) {
        Objects.requireNonNull(name, "name");
        try (InputStream in = AppConfig.class.getClassLoader().getResourceAsStream(name)) {
            if (in == null) {
                SimpleLog.log("config resource not found on classpath: " + name + "; using defaults");
                return fromProperties(new Properties());
            }
            return load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to read config resource: " + name, e);
        }
    }

    /** Загрузка из InputStream (пустой → defaults). */
    public static AppConfig load(InputStream in) throws IOException {
        Objects.requireNonNull(in, "in");
        Properties props = new Properties();
        props.load(in);
        return fromProperties(props);
    }

    /** Загрузка с пути в ФС. */
    public static AppConfig load(Path path) throws IOException {
        Objects.requireNonNull(path, "path");
        if (!Files.exists(path)) {
            SimpleLog.log("config file not found: " + path + "; using defaults");
            return fromProperties(new Properties());
        }
        try (InputStream in = Files.newInputStream(path)) {
            return load(in);
        }
    }

    /** Разбор Properties → валидированный AppConfig. */
    public static AppConfig fromProperties(Properties props) {
        Objects.requireNonNull(props, "props");

        Path logFile = resolveLogFile(props.getProperty("app.log.file", DEFAULT_LOG_FILE));
        SimpleLog.configure(logFile);

        long t1Ms = parsePositiveLong(props, "app.t1.ms", DEFAULT_T1_MS);
        long t2Ms = parsePositiveLong(props, "app.t2.ms", DEFAULT_T2_MS);
        int queueCapacity = parsePositiveInt(props, "app.queue.capacity", DEFAULT_QUEUE_CAPACITY);
        int reorderWindow = parsePositiveInt(props, "app.reorder.window", DEFAULT_REORDER_WINDOW);
        int workersConfigured = parseNonNegativeInt(props, "app.workers", DEFAULT_WORKERS);
        int writeRetryCount = parseNonNegativeInt(props, "app.write.retry.count", DEFAULT_WRITE_RETRY_COUNT);
        long writeRetryDelayMs = parseNonNegativeLong(props, "app.write.retry.delay.ms", DEFAULT_WRITE_RETRY_DELAY_MS);
        boolean metricsEnabled = parseBoolean(props, "app.metrics.enabled", DEFAULT_METRICS_ENABLED);
        long metricsSampleMs = parsePositiveLong(props, "app.metrics.sample.ms", DEFAULT_METRICS_SAMPLE_MS);

        String inDirRaw = props.getProperty("app.in.dir", DEFAULT_IN_DIR).trim();
        String outDirRaw = props.getProperty("app.out.dir", DEFAULT_OUT_DIR).trim();
        if (inDirRaw.isEmpty()) {
            inDirRaw = DEFAULT_IN_DIR;
        }
        if (outDirRaw.isEmpty()) {
            outDirRaw = DEFAULT_OUT_DIR;
        }
        Path inDir = resolveDir(inDirRaw);
        Path outDir = resolveDir(outDirRaw);

        if (t2Ms <= t1Ms) {
            SimpleLog.log("WARNING: app.t2.ms (" + t2Ms + ") <= app.t1.ms (" + t1Ms
                    + "); backlog/parallelism may not be needed, continuing anyway");
        }

        int workersEffective = computeWorkersEffective(workersConfigured, t1Ms, t2Ms);
        if (workersConfigured > 0) {
            SimpleLog.log("using configured workers=" + workersConfigured
                    + " (auto would be " + computeWorkersEffective(0, t1Ms, t2Ms) + ")");
        }

        return new AppConfig(
                t1Ms,
                t2Ms,
                inDir,
                outDir,
                queueCapacity,
                reorderWindow,
                workersConfigured,
                workersEffective,
                writeRetryCount,
                writeRetryDelayMs,
                logFile,
                metricsEnabled,
                metricsSampleMs);
    }

    /** Эффективное число workers: конфиг &gt; 0 или ceil(t2/t1). */
    static int computeWorkersEffective(int workersConfigured, long t1Ms, long t2Ms) {
        if (workersConfigured > 0) {
            return workersConfigured;
        }
        return Math.max(1, (int) Math.ceil((double) t2Ms / (double) t1Ms));
    }

    /**
     * Путь к логу из конфига + префикс {@code YYYYMMDDHHMMSS_} в имени файла
     * ({@code logs/pipeline.log} → {@code logs/20261008200600_pipeline.log}).
     */
    private static Path resolveLogFile(String raw) {
        String pathRaw = (raw == null || raw.isBlank()) ? DEFAULT_LOG_FILE : raw.trim();
        return SimpleLog.withTimestampPrefix(resolveDir(pathRaw));
    }

    /** Абсолютный путь или относительно {@code user.dir}. */
    private static Path resolveDir(String raw) {
        Path p = Path.of(raw);
        if (p.isAbsolute()) {
            return p.normalize();
        }
        return Path.of(System.getProperty("user.dir")).resolve(p).normalize();
    }

    private static boolean parseBoolean(Properties props, String key, boolean defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        String v = raw.trim().toLowerCase();
        if ("true".equals(v) || "yes".equals(v) || "1".equals(v)) {
            return true;
        }
        if ("false".equals(v) || "no".equals(v) || "0".equals(v)) {
            return false;
        }
        throw new IllegalStateException("Invalid config: " + key + " must be true/false, got '" + raw.trim() + "'");
    }

    private static long parsePositiveLong(Properties props, String key, long defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        long value = parseLongStrict(key, raw.trim());
        if (value <= 0) {
            throw new IllegalStateException("Invalid config: " + key + " must be > 0, got " + value);
        }
        return value;
    }

    private static int parsePositiveInt(Properties props, String key, int defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        int value = parseIntStrict(key, raw.trim());
        if (value <= 0) {
            throw new IllegalStateException("Invalid config: " + key + " must be > 0, got " + value);
        }
        return value;
    }

    private static int parseNonNegativeInt(Properties props, String key, int defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        int value = parseIntStrict(key, raw.trim());
        if (value < 0) {
            throw new IllegalStateException("Invalid config: " + key + " must be >= 0, got " + value);
        }
        return value;
    }

    private static long parseNonNegativeLong(Properties props, String key, long defaultValue) {
        String raw = props.getProperty(key);
        if (raw == null || raw.isBlank()) {
            return defaultValue;
        }
        long value = parseLongStrict(key, raw.trim());
        if (value < 0) {
            throw new IllegalStateException("Invalid config: " + key + " must be >= 0, got " + value);
        }
        return value;
    }

    private static long parseLongStrict(String key, String raw) {
        try {
            return Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid config: " + key + " is not a number: '" + raw + "'", e);
        }
    }

    private static int parseIntStrict(String key, String raw) {
        try {
            return Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Invalid config: " + key + " is not a number: '" + raw + "'", e);
        }
    }

    @Override
    public String toString() {
        return "AppConfig{"
                + "t1Ms=" + t1Ms
                + ", t2Ms=" + t2Ms
                + ", inDir=" + inDir
                + ", outDir=" + outDir
                + ", queueCapacity=" + queueCapacity
                + ", reorderWindow=" + reorderWindow
                + ", workersConfigured=" + workersConfigured
                + ", workersEffective=" + workersEffective
                + ", writeRetryCount=" + writeRetryCount
                + ", writeRetryDelayMs=" + writeRetryDelayMs
                + ", logFile=" + logFile
                + ", metricsEnabled=" + metricsEnabled
                + ", metricsSampleMs=" + metricsSampleMs
                + '}';
    }
}
