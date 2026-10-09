package com.renaissance.pipeline;

import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.metrics.MetricsSnapshot;
import com.renaissance.pipeline.pipeline.Pipeline;
import com.renaissance.pipeline.util.SimpleLog;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Точка входа: конфиг, подготовка In/Out, запуск Reader→Workers→Reorder→Writer, graceful shutdown.
 */
public final class App {

    private App() {
    }

    /** Запуск приложения и выход с кодом {@link #run(String[])}. */
    public static void main(String[] args) {
        System.exit(run(args));
    }

    /** Запуск пайплайна без {@code System.exit} (для тестов и программного вызова). */
    static int run(String[] args) {
        AppConfig cfg;
        try {
            cfg = AppConfig.load();
        } catch (IllegalStateException e) {
            SimpleLog.log("invalid configuration: " + e.getMessage());
            return 1;
        }

        if (!Files.isDirectory(cfg.inDir())) {
            SimpleLog.log("input directory does not exist: " + cfg.inDir()
                    + " (create In/ next to the working directory; it is not created automatically)");
            return 1;
        }

        try {
            Files.createDirectories(cfg.outDir());
            // Out policy on start: clear previous *.json so each run is a clean batch (no mid-seq resume).
            clearOutJson(cfg.outDir());
            SimpleLog.log("Out dir ready (cleared *.json): " + cfg.outDir());
        } catch (Exception e) {
            SimpleLog.log("cannot prepare Out directory " + cfg.outDir() + ": " + e.getMessage());
            return 1;
        }

        SimpleLog.log(String.format(
                "started t1=%d t2=%d workers=%d queueCapacity=%d reorderWindow=%d In=%s Out=%s log=%s",
                cfg.t1Ms(),
                cfg.t2Ms(),
                cfg.workersEffective(),
                cfg.queueCapacity(),
                cfg.reorderWindow(),
                cfg.inDir(),
                cfg.outDir(),
                cfg.logFile()));

        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.startFull(cfg, cfg.inDir(), cfg.outDir(), buffer);
        AtomicReference<Pipeline> pipelineRef = new AtomicReference<>(pipeline);
        long startedAt = System.currentTimeMillis();

        Thread hook = new Thread(() -> {
            Pipeline p = pipelineRef.get();
            if (p != null) {
                SimpleLog.log("shutdown hook: requesting stop");
                p.requestStop();
                try {
                    p.awaitCompletion(15, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    SimpleLog.log("shutdown hook interrupted while awaiting completion");
                }
                logSummary(p, startedAt);
            }
        }, "shutdown-hook");
        Runtime.getRuntime().addShutdownHook(hook);

        try {
            pipeline.awaitCompletion(24, TimeUnit.HOURS);
            pipelineRef.set(null);
            try {
                Runtime.getRuntime().removeShutdownHook(hook);
            } catch (IllegalStateException ignored) {
                // JVM already shutting down
            }
            logSummary(pipeline, startedAt);
            return exitCode(pipeline);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            SimpleLog.log("pipeline interrupted: " + e.getMessage(), e);
            pipeline.requestStop();
            try {
                pipeline.awaitCompletion(15, TimeUnit.SECONDS);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
            logSummary(pipeline, startedAt);
            return Math.max(1, exitCode(pipeline));
        }
    }

    /**
     * Код выхода: 0 — ок; 2 — fatal Writer/abort; 3 — рассинхрон assigned ≠ written+tombstones.
     */
    private static int exitCode(Pipeline pipeline) {
        if (pipeline.fileWriter() == null) {
            return 0;
        }
        if (pipeline.fileWriter().hasFailed() || pipeline.buffer().isAborted()) {
            return 2;
        }
        long assigned = pipeline.fileReader().getAssignedCount();
        long written = pipeline.fileWriter().getWrittenCount();
        long tombs = pipeline.fileWriter().getSkippedCount();
        if (assigned != written + tombs) {
            SimpleLog.log(String.format(
                    "integrity mismatch: assigned=%d written=%d tombstones=%d (expected written+tombstones==assigned)",
                    assigned, written, tombs));
            return 3;
        }
        return 0;
    }

    /** Краткая сводка и таблица метрик в лог при завершении. */
    private static void logSummary(Pipeline pipeline, long startedAtMs) {
        long written = pipeline.fileWriter() != null ? pipeline.fileWriter().getWrittenCount() : -1;
        long writerSkipped = pipeline.fileWriter() != null ? pipeline.fileWriter().getSkippedCount() : -1;
        boolean failed = pipeline.fileWriter() != null && pipeline.fileWriter().hasFailed();
        long durationMs = Math.max(0L, System.currentTimeMillis() - startedAtMs);
        SimpleLog.log(String.format(
                "shutdown summary assigned=%d skippedIn=%d written=%d tombstones=%d writerFailed=%s aborted=%s durationMs=%d",
                pipeline.fileReader().getAssignedCount(),
                pipeline.fileReader().getSkippedCount(),
                written,
                writerSkipped,
                failed,
                pipeline.buffer().isAborted(),
                durationMs));
        MetricsSnapshot snap = pipeline.metricsSnapshot();
        if (snap != null) {
            for (String line : snap.formatTable().split("\\R", -1)) {
                if (!line.isEmpty()) {
                    SimpleLog.log(line);
                }
            }
        }
    }

    /** Очистка {@code *.json} в Out/ перед стартом (чистый batch без resume). */
    static void clearOutJson(Path outDir) throws IOException {
        if (!Files.isDirectory(outDir)) {
            return;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(outDir, "*.json")) {
            for (Path path : stream) {
                Files.deleteIfExists(path);
            }
        }
    }
}
