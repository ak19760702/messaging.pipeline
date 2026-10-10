package com.renaissance.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.pipeline.Pipeline;
import com.renaissance.pipeline.util.JsonSupport;
import com.renaissance.pipeline.writer.FileWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PipelineEdgeIT {

    @TempDir
    Path tempDir;

    @Test
    void emptyIn_completesWithZeroOut() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        AppConfig cfg = cfg(in, out, 1L, 2L, 2, 8, 16);
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, new ReorderBuffer(cfg.reorderWindow()));
        pipeline.awaitCompletion(10, TimeUnit.SECONDS);

        assertEquals(0, pipeline.fileReader().getAssignedCount());
        assertEquals(0, pipeline.fileWriter().getWrittenCount());
        assertTrue(listOutJson(out).isEmpty());
    }

    @Test
    void chaosBrokenJson_outOnlyValidInOrder() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        write(in, "001.json", "{\"id\":1}");
        write(in, "002.json", "{nope");
        write(in, "003.json", "");
        write(in, "004.json", "[]");
        write(in, "005.json", "{\"id\":5}");
        write(in, "006.json", "null");
        write(in, "007.json", "{\"id\":7}");
        write(in, "notes.txt", "ignore");

        AppConfig cfg = cfg(in, out, 1L, 3L, 3, 4, 16);
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, new ReorderBuffer(cfg.reorderWindow()));
        pipeline.awaitCompletion(20, TimeUnit.SECONDS);

        List<Path> outs = listOutJson(out);
        assertEquals(3, outs.size());
        assertEquals(3, pipeline.fileReader().getAssignedCount());
        assertEquals(4, pipeline.fileReader().getSkippedCount()); // broken, empty, array, null

        int[] expectedIds = {1, 5, 7};
        for (int i = 0; i < 3; i++) {
            assertEquals(FileWriter.paddedName(i), outs.get(i).getFileName().toString());
            JsonNode n = JsonSupport.mapper().readTree(Files.readString(outs.get(i)));
            assertEquals(expectedIds[i], n.get("id").asInt());
            assertTrue(n.has("processedAt"));
        }
    }

    @Test
    void workerFail_tombstoneOmitsSeqFromOut() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        write(in, "001.json", "{\"id\":1}");
        write(in, "002.json", "{\"id\":2}");
        write(in, "003.json", "{\"id\":3}");

        AppConfig cfg = cfg(in, out, 1L, 2L, 2, 8, 16);
        Pipeline pipeline = Pipeline.startFull(
                cfg, in, out, new ReorderBuffer(cfg.reorderWindow()), m -> m.seq() == 1L);
        pipeline.awaitCompletion(15, TimeUnit.SECONDS);

        List<Path> outs = listOutJson(out);
        assertEquals(2, outs.size());
        assertFalse(Files.exists(out.resolve("000001.json")));
        assertTrue(Files.exists(out.resolve("000000.json")));
        assertTrue(Files.exists(out.resolve("000002.json")));
        assertEquals(1, pipeline.fileWriter().getSkippedCount());
        assertEquals(2, pipeline.fileWriter().getWrittenCount());
    }

    @Test
    void smallQueueAndWindow_stillCompletes() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        for (int i = 1; i <= 12; i++) {
            write(in, String.format("%03d.json", i), "{\"n\":" + i + "}");
        }

        // capacity=1, window=2, 3 workers, t2>t1 → backpressure on queue + ring
        AppConfig cfg = cfg(in, out, 1L, 5L, 3, 1, 2);
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, new ReorderBuffer(cfg.reorderWindow()));
        pipeline.awaitCompletion(25, TimeUnit.SECONDS);

        assertEquals(12, pipeline.fileReader().getAssignedCount());
        assertEquals(12, pipeline.fileWriter().getWrittenCount());
        assertEquals(12, listOutJson(out).size());
        for (int i = 0; i < 12; i++) {
            assertTrue(Files.exists(out.resolve(FileWriter.paddedName(i))));
        }
    }

    /**
     * Очередь на 1 слот, один воркер, t2=6 с: Reader не успевает положить poison-pill за свои 5 с.
     * Раньше воркер навсегда оставался на queue.take() и awaitCompletion висел; теперь
     * Pipeline дошлёт недостающий маркер и завершится сам.
     */
    @Test
    @Timeout(value = 40, unit = TimeUnit.SECONDS)
    void slowProcessingFullQueue_missingPoisonPillDeliveredByPipeline() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);
        write(in, "001.json", "{\"id\":1}");
        write(in, "002.json", "{\"id\":2}");

        AppConfig cfg = cfg(in, out, 1L, 6000L, 1, 1, 16);
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, new ReorderBuffer(cfg.reorderWindow()));
        pipeline.awaitCompletion(30, TimeUnit.SECONDS);

        assertEquals(0, pipeline.fileReader().getPoisonSentCount()); // Reader не успел сам
        assertEquals(2, pipeline.fileReader().getAssignedCount());
        assertEquals(2, pipeline.fileWriter().getWrittenCount());
        assertEquals(2, listOutJson(out).size());
    }

    @Test
    void outDirCreatedIfMissing() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("nested").resolve("Out");
        Files.createDirectories(in);
        write(in, "001.json", "{\"id\":1}");
        assertFalse(Files.exists(out));

        AppConfig cfg = cfg(in, out, 0L, 1L, 1, 4, 8);
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, new ReorderBuffer(cfg.reorderWindow()));
        pipeline.awaitCompletion(10, TimeUnit.SECONDS);

        assertTrue(Files.isDirectory(out));
        assertTrue(Files.exists(out.resolve("000000.json")));
    }

    private static List<Path> listOutJson(Path out) throws Exception {
        if (!Files.isDirectory(out)) {
            return List.of();
        }
        try (Stream<Path> s = Files.list(out)) {
            List<Path> list = new ArrayList<>();
            s.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .forEach(list::add);
            return list;
        }
    }

    private static void write(Path dir, String name, String content) throws Exception {
        Files.writeString(dir.resolve(name), content, StandardCharsets.UTF_8);
    }

    private static AppConfig cfg(
            Path in, Path out, long t1, long t2, int workers, int queueCap, int window) {
        return new AppConfig(
                t1, t2,
                in, out,
                queueCap, window,
                workers, workers,
                3, 5L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }
}
