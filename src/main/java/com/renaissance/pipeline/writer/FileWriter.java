package com.renaissance.pipeline.writer;

import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.buffer.ReorderBuffer.TakeResult;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.metrics.PipelineMetrics;
import com.renaissance.pipeline.model.Message;
import com.renaissance.pipeline.util.JsonSupport;
import com.renaissance.pipeline.util.SimpleLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Однопоточный потребитель {@link ReorderBuffer}: DATA → Out/{@code %06d.json},
 * TOMBSTONE — пропуск, End — выход. После успешной записи — sleep(t1).
 * IOExceptions ретраятся {@code app.write.retry.count} раз.
 */
public final class FileWriter implements Runnable {

    /** Запись одного сообщения в путь (инъекция для тестов retry). */
    @FunctionalInterface
    public interface MessageSink {
        void write(Message message, Path target) throws IOException;
    }

    private final AppConfig cfg;
    private final ReorderBuffer buffer;
    private final Path outDir;
    private final MessageSink sink;
    private final PipelineMetrics metrics; // nullable
    private final AtomicBoolean failed = new AtomicBoolean(false);
    private volatile Runnable onFatal;

    private long writtenCount;
    private long skippedCount;

    /** Writer с Out/ из конфига и {@link JsonSupport#write}. */
    public FileWriter(AppConfig cfg, ReorderBuffer buffer) {
        this(cfg, cfg.outDir(), buffer, JsonSupport::write, null);
    }

    /** Writer с явным Out/. */
    public FileWriter(AppConfig cfg, Path outDir, ReorderBuffer buffer) {
        this(cfg, outDir, buffer, JsonSupport::write, null);
    }

    /** Writer с кастомным sink. */
    public FileWriter(AppConfig cfg, Path outDir, ReorderBuffer buffer, MessageSink sink) {
        this(cfg, outDir, buffer, sink, null);
    }

    /** Полный конструктор с метриками. */
    public FileWriter(
            AppConfig cfg,
            Path outDir,
            ReorderBuffer buffer,
            MessageSink sink,
            PipelineMetrics metrics) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.outDir = Objects.requireNonNull(outDir, "outDir");
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        this.sink = Objects.requireNonNull(sink, "sink");
        this.metrics = metrics;
    }

    /** Колбэк при остановке Writer из-за исчерпанных retry / fatal IO. */
    public void setOnFatal(Runnable onFatal) {
        this.onFatal = onFatal;
    }

    /** Число успешно записанных файлов. */
    public long getWrittenCount() {
        return writtenCount;
    }

    /** Число пропущенных TOMBSTONE. */
    public long getSkippedCount() {
        return skippedCount;
    }

    /** Writer остановился из-за невосстановимой ошибки записи. */
    public boolean hasFailed() {
        return failed.get();
    }

    /** Цикл take → write/skip до End. */
    @Override
    public void run() {
        try {
            Files.createDirectories(outDir);
            while (true) {
                TakeResult result = buffer.take();
                if (result instanceof TakeResult.End) {
                    SimpleLog.log("FileWriter finished: written=" + writtenCount + " skipped=" + skippedCount);
                    return;
                }
                if (result instanceof TakeResult.Tombstone) {
                    skippedCount++;
                    SimpleLog.log("Writer skipped TOMBSTONE (no Out file); cursor advanced; skippedTotal="
                            + skippedCount);
                    continue;
                }
                if (result instanceof TakeResult.Data data) {
                    Message message = data.message();
                    writeWithRetry(message);
                    sleepT1();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            SimpleLog.log("FileWriter interrupted: written=" + writtenCount + " skipped=" + skippedCount);
        } catch (IOException e) {
            markFailed(e);
        } catch (RuntimeException e) {
            markFailed(e);
        }
    }

    /** Пометить fatal, abort buffer и вызвать onFatal. */
    private void markFailed(Exception e) {
        failed.set(true);
        SimpleLog.log("FileWriter fatal error after retries, stopping: " + e.getMessage(), e);
        buffer.abort();
        Runnable cb = onFatal;
        if (cb != null) {
            try {
                cb.run();
            } catch (RuntimeException re) {
                SimpleLog.log("onFatal callback failed: " + re.getMessage(), re);
            }
        }
    }

    /**
     * Запись с ретраями: до {@code writeRetryCount + 1} попыток, пауза {@code writeRetryDelayMs}.
     */
    private void writeWithRetry(Message message) throws IOException, InterruptedException {
        Path target = outDir.resolve(paddedName(message.seq()));
        int maxAttempts = cfg.writeRetryCount() + 1;
        IOException last = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                sink.write(message, target);
                writtenCount++;
                if (metrics != null) {
                    metrics.recordDeparted();
                }
                SimpleLog.log("Writer wrote seq=" + message.seq() + " → " + target.getFileName()
                        + (attempt > 1 ? " (after " + (attempt - 1) + " retry(ies))" : ""));
                return;
            } catch (IOException e) {
                last = e;
                SimpleLog.log("Write failed for seq=" + message.seq()
                        + " attempt " + attempt + "/" + maxAttempts + ": " + e.getMessage());
                if (attempt < maxAttempts) {
                    long delay = cfg.writeRetryDelayMs();
                    if (delay > 0) {
                        Thread.sleep(delay);
                    }
                }
            }
        }
        throw new IOException(
                "Write exhausted retries (" + cfg.writeRetryCount() + ") for seq=" + message.seq()
                        + " → " + target.getFileName(),
                last);
    }

    /** Пауза t1 после успешной записи DATA. */
    private void sleepT1() throws InterruptedException {
        long ms = cfg.t1Ms();
        if (ms > 0) {
            Thread.sleep(ms);
        }
    }

    /** Имя файла Out по seq: {@code 000000.json}, {@code 000001.json}, … */
    public static String paddedName(long seq) {
        return String.format("%06d.json", seq);
    }
}
