package com.renaissance.pipeline.worker;

import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.metrics.PipelineMetrics;
import com.renaissance.pipeline.model.Message;
import com.renaissance.pipeline.util.SimpleLog;

import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/**
 * Берёт сообщения из очереди, sleep(t2), ставит {@code processedAt}, кладёт в {@link ReorderBuffer}.
 * Сбой после назначения seq → tombstone; poison-pill → выход из цикла.
 */
public final class ProcessingWorker implements Runnable {

    private final AppConfig cfg;
    private final BlockingQueue<Message> queue;
    private final ReorderBuffer buffer;
    /** Когда true — бросает после processedAt, чтобы проверить путь tombstone (тесты). */
    private final Predicate<Message> failAfterProcess;
    private final PipelineMetrics metrics; // nullable

    /** Worker без инъекции сбоев и без метрик. */
    public ProcessingWorker(AppConfig cfg, BlockingQueue<Message> queue, ReorderBuffer buffer) {
        this(cfg, queue, buffer, m -> false, null);
    }

    /**
     * Worker с хуком: если {@code failAfterProcess} вернул true — исключение до buffer.put → tombstone.
     */
    public ProcessingWorker(
            AppConfig cfg,
            BlockingQueue<Message> queue,
            ReorderBuffer buffer,
            Predicate<Message> failAfterProcess) {
        this(cfg, queue, buffer, failAfterProcess, null);
    }

    /** Полный конструктор с метриками. */
    public ProcessingWorker(
            AppConfig cfg,
            BlockingQueue<Message> queue,
            ReorderBuffer buffer,
            Predicate<Message> failAfterProcess,
            PipelineMetrics metrics) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.queue = Objects.requireNonNull(queue, "queue");
        this.buffer = Objects.requireNonNull(buffer, "buffer");
        this.failAfterProcess = Objects.requireNonNull(failAfterProcess, "failAfterProcess");
        this.metrics = metrics;
    }

    /** Цикл: take → t2 → put/tombstone; выход по poison или interrupt. */
    @Override
    public void run() {
        while (true) {
            Message message;
            try {
                message = queue.take();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                SimpleLog.log("ProcessingWorker interrupted while taking from queue");
                return;
            }

            if (message.isPoison()) {
                return;
            }

            long startedNanos = System.nanoTime();
            if (metrics != null) {
                metrics.onWorkerTake();
            }
            long seq = message.seq();
            try {
                sleepT2();
                message.setProcessedAt(System.currentTimeMillis());
                if (failAfterProcess.test(message)) {
                    throw new RuntimeException("injected processing failure for seq=" + seq);
                }
                buffer.put(seq, message);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                tombstoneSafely(seq, e);
                return;
            } catch (Exception e) {
                SimpleLog.log("Worker failed seq=" + seq + "; putting tombstone: " + e.getMessage(), e);
                tombstoneSafely(seq, e);
            } finally {
                if (metrics != null) {
                    long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startedNanos);
                    metrics.onWorkerComplete(latencyMs);
                }
            }
        }
    }

    /** Имитация обработки: sleep(t2). */
    private void sleepT2() throws InterruptedException {
        long ms = cfg.t2Ms();
        if (ms > 0) {
            Thread.sleep(ms);
        }
    }

    /**
     * Закрыть слот seq tombstone'ом или abort buffer, чтобы Writer не завис на «дыре».
     */
    private void tombstoneSafely(long seq, Exception cause) {
        try {
            buffer.putTombstone(seq);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            SimpleLog.log("Interrupted while putting tombstone for seq=" + seq
                    + "; aborting ReorderBuffer", ie);
            buffer.abort();
        } catch (Exception te) {
            SimpleLog.log("Failed to put tombstone for seq=" + seq
                    + " (original cause: " + cause.getMessage() + "); aborting ReorderBuffer", te);
            buffer.abort();
        }
    }
}
