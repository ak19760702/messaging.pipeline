package com.renaissance.pipeline.pipeline;

import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.metrics.MetricsSampler;
import com.renaissance.pipeline.metrics.MetricsSnapshot;
import com.renaissance.pipeline.metrics.PipelineMetrics;
import com.renaissance.pipeline.model.Message;
import com.renaissance.pipeline.queue.ProcessingQueue;
import com.renaissance.pipeline.reader.FileReader;
import com.renaissance.pipeline.util.SimpleLog;
import com.renaissance.pipeline.util.JsonSupport;
import com.renaissance.pipeline.worker.ProcessingWorker;
import com.renaissance.pipeline.writer.FileWriter;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;

/**
 * Оркестрация пайплайна.
 * <ul>
 *   <li>{@link #start(AppConfig, Path, ReorderBuffer)} — Reader→Workers→Buffer (тесты дренируют buffer).</li>
 *   <li>{@link #startFull(AppConfig, Path, Path, ReorderBuffer)} — + FileWriter (In→Out).</li>
 * </ul>
 */
public final class Pipeline {

    private final FileReader fileReader;
    private final Thread readerThread;
    private final ExecutorService workerPool;
    private final ReorderBuffer buffer;
    private final FileWriter fileWriter;       // null if writer not started
    private final Thread writerThread;        // null if writer not started
    private final int workerCount;
    private final PipelineMetrics metrics;    // null if metrics disabled
    private final MetricsSampler sampler;     // null if metrics disabled
    private final BlockingQueue<Message> queue; // очередь Reader → Workers (для досылки poison-pill)
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);
    private final AtomicBoolean completionStarted = new AtomicBoolean(false);
    private final CountDownLatch completionDone = new CountDownLatch(1);
    private volatile boolean completionFailed;

    private Pipeline(
            FileReader fileReader,
            Thread readerThread,
            ExecutorService workerPool,
            ReorderBuffer buffer,
            FileWriter fileWriter,
            Thread writerThread,
            int workerCount,
            PipelineMetrics metrics,
            MetricsSampler sampler,
            BlockingQueue<Message> queue) {
        this.queue = queue;
        this.fileReader = fileReader;
        this.readerThread = readerThread;
        this.workerPool = workerPool;
        this.buffer = buffer;
        this.fileWriter = fileWriter;
        this.writerThread = writerThread;
        this.workerCount = workerCount;
        this.metrics = metrics;
        this.sampler = sampler;
    }

    /** Старт Reader→Workers→Buffer без FileWriter. */
    public static Pipeline start(AppConfig cfg, Path inDir, ReorderBuffer buffer) {
        return start(cfg, inDir, buffer, m -> false);
    }

    /**
     * Старт без Writer; {@code failAfterProcess} — хук отказа после обработки (тесты/chaos).
     */
    public static Pipeline start(
            AppConfig cfg,
            Path inDir,
            ReorderBuffer buffer,
            Predicate<Message> failAfterProcess) {
        return startInternal(cfg, inDir, null, buffer, failAfterProcess, false);
    }

    /** Полный пайплайн In→Out с FileWriter. */
    public static Pipeline startFull(AppConfig cfg, Path inDir, Path outDir, ReorderBuffer buffer) {
        return startFull(cfg, inDir, outDir, buffer, m -> false);
    }

    /** Полный пайплайн с опциональным хуком отказа worker'а. */
    public static Pipeline startFull(
            AppConfig cfg,
            Path inDir,
            Path outDir,
            ReorderBuffer buffer,
            Predicate<Message> failAfterProcess) {
        Objects.requireNonNull(outDir, "outDir");
        return startInternal(cfg, inDir, outDir, buffer, failAfterProcess, true);
    }

    /** Создание очереди, пула worker'ов, Reader/Writer/sampler и запуск потоков. */
    private static Pipeline startInternal(
            AppConfig cfg,
            Path inDir,
            Path outDir,
            ReorderBuffer buffer,
            Predicate<Message> failAfterProcess,
            boolean withWriter) {
        Objects.requireNonNull(cfg, "cfg");
        Objects.requireNonNull(inDir, "inDir");
        Objects.requireNonNull(buffer, "buffer");
        Objects.requireNonNull(failAfterProcess, "failAfterProcess");

        int n = cfg.workersEffective();
        ProcessingQueue queue = new ProcessingQueue(cfg.queueCapacity());

        PipelineMetrics metrics = cfg.metricsEnabled() ? new PipelineMetrics() : null;

        ExecutorService pool = Executors.newFixedThreadPool(n, r -> {
            Thread t = new Thread(r);
            t.setName("processing-worker");
            t.setDaemon(false);
            return t;
        });
        for (int i = 0; i < n; i++) {
            pool.submit(new ProcessingWorker(
                    cfg, queue.asBlockingQueue(), buffer, failAfterProcess, metrics));
        }

        FileReader reader = new FileReader(cfg, inDir, queue.asBlockingQueue(), n, metrics);
        Thread readerThread = new Thread(reader, "file-reader");

        FileWriter writer = null;
        Thread writerThread = null;
        Pipeline[] self = new Pipeline[1];
        if (withWriter) {
            writer = new FileWriter(cfg, outDir, buffer, JsonSupport::write, metrics);
            writerThread = new Thread(writer, "file-writer");
        }

        MetricsSampler sampler = null;
        if (metrics != null) {
            sampler = new MetricsSampler(
                    metrics,
                    queue::size,
                    metrics::inFlight,
                    buffer::occupied,
                    cfg.metricsSampleMs());
        }

        Pipeline pipeline = new Pipeline(
                reader, readerThread, pool, buffer, writer, writerThread, n, metrics, sampler,
                queue.asBlockingQueue());
        self[0] = pipeline;
        if (writer != null) {
            writer.setOnFatal(() -> {
                SimpleLog.log("Writer fatal — requesting pipeline stop");
                self[0].requestStop();
                pool.shutdownNow();
            });
            writerThread.start();
        }

        if (sampler != null) {
            sampler.start();
        }

        readerThread.start();

        SimpleLog.log("Pipeline started: workers=" + n + " queueCapacity=" + cfg.queueCapacity()
                + " inDir=" + inDir + " withWriter=" + withWriter
                + (withWriter ? " outDir=" + outDir : "")
                + " metrics=" + (metrics != null));
        return pipeline;
    }

    /** FileReader этого пайплайна. */
    public FileReader fileReader() {
        return fileReader;
    }

    /** FileWriter или {@code null}, если Writer не запускался. */
    public FileWriter fileWriter() {
        return fileWriter;
    }

    /** Есть ли FileWriter. */
    public boolean hasWriter() {
        return fileWriter != null;
    }

    /** ReorderBuffer пайплайна. */
    public ReorderBuffer buffer() {
        return buffer;
    }

    /** Число worker-потоков. */
    public int workerCount() {
        return workerCount;
    }

    /** Метрики или {@code null}, если отключены. */
    public PipelineMetrics metrics() {
        return metrics;
    }

    /** Снимок метрик после остановки sampler; {@code null}, если метрики выключены. */
    public MetricsSnapshot metricsSnapshot() {
        return metrics == null ? null : metrics.snapshot();
    }

    /**
     * Мягкая остановка: Reader не берёт новые файлы; очередь дочищается;
     * worker'ы завершаются / получают poison; buffer — signalEnd; Writer дренирует.
     */
    public void requestStop() {
        if (stopRequested.compareAndSet(false, true)) {
            SimpleLog.log("Pipeline stop requested");
            fileReader.requestStop();
            readerThread.interrupt();
        }
    }

    /**
     * Ожидание завершения: join Reader, пул worker'ов, signalEnd/abort, join Writer.
     * Параллельные waiters ждут общий latch — без частичного drain.
     */
    public void awaitCompletion(long timeout, TimeUnit unit) throws InterruptedException {
        if (!completionStarted.compareAndSet(false, true)) {
            if (!completionDone.await(timeout, unit)) {
                throw new InterruptedException("timeout waiting for pipeline completion");
            }
            if (completionFailed) {
                throw new InterruptedException("pipeline completion failed in another waiter");
            }
            return;
        }

        try {
            runCompletion(System.nanoTime() + unit.toNanos(timeout));
        } catch (InterruptedException e) {
            completionFailed = true;
            throw e;
        } finally {
            completionDone.countDown();
        }
    }

    /** Последовательное завершение Reader → workers → buffer → Writer в пределах deadline. */
    private void runCompletion(long deadlineNanos) throws InterruptedException {
        long remaining = deadlineNanos - System.nanoTime();
        if (remaining <= 0) {
            throw new InterruptedException("timeout waiting for reader");
        }
        readerThread.join(joinMillis(remaining));
        if (readerThread.isAlive()) {
            fileReader.requestStop();
            readerThread.interrupt();
            remaining = deadlineNanos - System.nanoTime();
            if (remaining > 0) {
                readerThread.join(joinMillis(remaining));
            }
            if (readerThread.isAlive()) {
                stopSampler();
                throw new InterruptedException("timeout waiting for FileReader");
            }
        }

        deliverMissingPoisonPills(deadlineNanos);

        workerPool.shutdown();
        remaining = deadlineNanos - System.nanoTime();
        boolean workersClean = remaining > 0
                && workerPool.awaitTermination(remaining, TimeUnit.NANOSECONDS);
        if (!workersClean) {
            workerPool.shutdownNow();
            remaining = deadlineNanos - System.nanoTime();
            workersClean = remaining > 0
                    && workerPool.awaitTermination(Math.max(remaining, 1), TimeUnit.NANOSECONDS);
        }

        if (workersClean && !buffer.isAborted()) {
            buffer.signalEnd();
            SimpleLog.log("Pipeline workers done; ReorderBuffer.signalEnd(); assigned="
                    + fileReader.getAssignedCount() + " skipped=" + fileReader.getSkippedCount());
        } else {
            buffer.abort();
            SimpleLog.log("Pipeline workers unclean or buffer already aborted; ReorderBuffer.abort(); assigned="
                    + fileReader.getAssignedCount() + " skipped=" + fileReader.getSkippedCount());
        }

        if (writerThread != null) {
            remaining = deadlineNanos - System.nanoTime();
            try {
                joinWriter(remaining);
                SimpleLog.log("Pipeline complete: written=" + fileWriter.getWrittenCount()
                        + " writerSkipped=" + fileWriter.getSkippedCount());
            } finally {
                stopSampler();
            }
        } else {
            stopSampler();
        }

        if (!workersClean) {
            throw new InterruptedException("timeout waiting for workers");
        }
    }

    /**
     * Если Reader не успел положить все poison-pill (очередь была полна дольше 5 с),
     * дослать недостающие: иначе воркер без маркера навсегда остаётся на queue.take().
     * Не ждём, если пул уже остановлен (fatal) или buffer в abort, и не дольше общего deadline.
     */
    private void deliverMissingPoisonPills(long deadlineNanos) throws InterruptedException {
        int missing = workerCount - fileReader.getPoisonSentCount();
        if (missing <= 0) {
            return;
        }
        SimpleLog.log("Pipeline: Reader delivered " + fileReader.getPoisonSentCount() + "/" + workerCount
                + " poison-pill(s); delivering the remaining " + missing);
        while (missing > 0 && !workerPool.isShutdown() && !buffer.isAborted()) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                return;
            }
            long waitNanos = Math.min(remaining, TimeUnit.MILLISECONDS.toNanos(200));
            if (queue.offer(Message.poisonPill(), waitNanos, TimeUnit.NANOSECONDS)) {
                missing--;
            }
        }
    }

    /** Остановка фонового sampler метрик. */
    private void stopSampler() {
        if (sampler != null) {
            sampler.stop();
        }
    }

    /** Join Writer с таймаутом; при просрочке — interrupt. */
    private void joinWriter(long remainingNanos) throws InterruptedException {
        if (remainingNanos <= 0) {
            throw new InterruptedException("timeout waiting for FileWriter");
        }
        writerThread.join(joinMillis(remainingNanos));
        if (writerThread.isAlive()) {
            writerThread.interrupt();
            throw new InterruptedException("timeout waiting for FileWriter");
        }
    }

    /** Миллисекунды для {@code join}: не отдавать 0 (вечное ожидание) при остатке &lt; 1 ms. */
    private static long joinMillis(long nanos) {
        if (nanos <= 0) {
            return 0;
        }
        long ms = TimeUnit.NANOSECONDS.toMillis(nanos);
        return ms == 0 ? 1 : ms;
    }

    /** Ожидание завершения с таймаутом 60 с. */
    public void awaitCompletion() throws InterruptedException {
        awaitCompletion(60, TimeUnit.SECONDS);
    }
}
