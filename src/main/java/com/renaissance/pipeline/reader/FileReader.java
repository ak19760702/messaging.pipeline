package com.renaissance.pipeline.reader;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.metrics.PipelineMetrics;
import com.renaissance.pipeline.model.Message;
import com.renaissance.pipeline.util.JsonSupport;
import com.renaissance.pipeline.util.SimpleLog;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Однопоточный сканер In/: список {@code *.json}, lex-сортировка, parse, seq, put в очередь, sleep(t1).
 * После batch — {@code poisonPillCount} poison-pill для выхода worker'ов.
 * Seq с 0; {@link #requestStop()} прекращает новые файлы, уже в очереди — дочищаются.
 */
public final class FileReader implements Runnable {

    private final AppConfig cfg;
    private final Path inDir;
    private final BlockingQueue<Message> queue;
    private final int poisonPillCount;
    private final PipelineMetrics metrics; // nullable
    private final AtomicBoolean stopRequested = new AtomicBoolean(false);

    private long nextSeq;
    private long assignedCount;
    private long skippedCount;

    /** Reader с In/ из конфига и числом poison = эффективные workers. */
    public FileReader(AppConfig cfg, BlockingQueue<Message> queue) {
        this(cfg, cfg.inDir(), queue, cfg.workersEffective(), null);
    }

    /** Reader с явным In/ и числом poison-pill. */
    public FileReader(AppConfig cfg, Path inDir, BlockingQueue<Message> queue, int poisonPillCount) {
        this(cfg, inDir, queue, poisonPillCount, null);
    }

    /** Полный конструктор с опциональными метриками. */
    public FileReader(
            AppConfig cfg,
            Path inDir,
            BlockingQueue<Message> queue,
            int poisonPillCount,
            PipelineMetrics metrics) {
        this.cfg = Objects.requireNonNull(cfg, "cfg");
        this.inDir = Objects.requireNonNull(inDir, "inDir");
        this.queue = Objects.requireNonNull(queue, "queue");
        if (poisonPillCount < 0) {
            throw new IllegalArgumentException("poisonPillCount must be >= 0, got " + poisonPillCount);
        }
        this.poisonPillCount = poisonPillCount;
        this.metrics = metrics;
    }

    /** Число назначенных seq. */
    public long getAssignedCount() {
        return assignedCount;
    }

    /** Число пропущенных файлов (битый JSON / недоступен). */
    public long getSkippedCount() {
        return skippedCount;
    }

    /** Следующий seq к назначению (= число успешно назначенных). */
    public long nextSeq() {
        return nextSeq;
    }

    /** Не брать новые файлы из In/; poison-pill всё равно отправляются. */
    public void requestStop() {
        stopRequested.set(true);
    }

    /** Запрошена ли остановка. */
    public boolean isStopRequested() {
        return stopRequested.get();
    }

    /** Список файлов In/ и обработка batch. */
    @Override
    public void run() {
        try {
            processListedFiles(listJsonFilesLexSorted(inDir));
        } catch (InterruptedException e) {
            SimpleLog.log("FileReader interrupted: " + e.getMessage());
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Обработка заранее собранного списка (для тестов исчезнувших файлов).
     * В finally всегда шлёт poison-pill.
     */
    void processListedFiles(List<Path> files) throws InterruptedException {
        try {
            for (Path file : files) {
                if (stopRequested.get() || Thread.currentThread().isInterrupted()) {
                    SimpleLog.log("FileReader stop requested / interrupted before finishing In/");
                    break;
                }
                processFile(file);
            }
        } catch (InterruptedException e) {
            SimpleLog.log("FileReader interrupted while processing In/: " + e.getMessage());
            Thread.currentThread().interrupt();
            throw e;
        } finally {
            sendPoisonPills();
        }
    }

    /** Чтение одного файла: parse → seq → put в очередь → sleep(t1); при ошибке — skip. */
    private void processFile(Path file) throws InterruptedException {
        String text;
        try {
            text = Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            skippedCount++;
            SimpleLog.log("SKIP: cannot read file (missing/unavailable): " + file.getFileName()
                    + " — " + e.getMessage());
            return;
        }

        Optional<ObjectNode> parsed = JsonSupport.parseObject(text);
        if (parsed.isEmpty()) {
            skippedCount++;
            SimpleLog.log("SKIP: bad/empty/non-object JSON, seq not assigned: " + file.getFileName());
            return;
        }

        long seq = nextSeq++;
        Message message = JsonSupport.fromParsed(parsed.get(), seq);
        queue.put(message);
        assignedCount++;
        if (metrics != null) {
            metrics.recordArrived();
        }
        SimpleLog.log("Reader assigned seq=" + seq + " for file=" + file.getFileName());
        sleepT1();
    }

    /**
     * Best-effort доставка poison-pill: timed offer, чтобы полная очередь не повесила Reader.
     */
    private void sendPoisonPills() {
        Thread.interrupted();
        int sent = 0;
        for (int i = 0; i < poisonPillCount; i++) {
            try {
                if (!queue.offer(Message.poisonPill(), 5, TimeUnit.SECONDS)) {
                    SimpleLog.log("FileReader: timeout offering poison pill "
                            + (i + 1) + "/" + poisonPillCount + "; workers may need shutdownNow");
                    break;
                }
                sent++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                SimpleLog.log("FileReader: interrupted offering poison pills after sent=" + sent);
                break;
            }
        }
        SimpleLog.log("FileReader finished In/; sent " + sent + "/" + poisonPillCount
                + " poison-pill(s); assigned=" + assignedCount + " skipped=" + skippedCount);
    }

    /** Пауза t1 между сообщениями; прерывается по requestStop. */
    private void sleepT1() throws InterruptedException {
        long ms = cfg.t1Ms();
        if (ms <= 0) {
            if (stopRequested.get()) {
                throw new InterruptedException("stop requested");
            }
            return;
        }
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(ms);
        while (System.nanoTime() < deadline) {
            if (stopRequested.get()) {
                throw new InterruptedException("stop requested");
            }
            long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadline - System.nanoTime());
            if (remainingMs <= 0) {
                break;
            }
            Thread.sleep(Math.min(5L, remainingMs));
        }
    }

    /** Список {@code *.json} в In/ в лексикографическом порядке имён. */
    static List<Path> listJsonFilesLexSorted(Path inDir) {
        List<Path> files = new ArrayList<>();
        if (!Files.isDirectory(inDir)) {
            SimpleLog.log("In dir is not a directory: " + inDir);
            return files;
        }
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(inDir)) {
            for (Path path : stream) {
                if (!Files.isRegularFile(path)) {
                    continue;
                }
                String name = path.getFileName().toString();
                if (name.toLowerCase().endsWith(".json")) {
                    files.add(path);
                }
            }
        } catch (IOException e) {
            SimpleLog.log("Failed to list In dir: " + inDir + " — " + e.getMessage(), e);
            return List.of();
        }
        files.sort(Comparator.comparing(p -> p.getFileName().toString()));
        return files;
    }
}
