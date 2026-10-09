package com.renaissance.pipeline;

import com.fasterxml.jackson.databind.JsonNode;
import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.pipeline.Pipeline;
import com.renaissance.pipeline.util.JsonSupport;
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
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 30, unit = TimeUnit.SECONDS)
class PipelineIT {

    @TempDir
    Path tempDir;

    @Test
    void inToOut_preservesOrderAndProcessedAt() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        write(in, "001.json", "{\"id\":1,\"text\":\"a\"}");
        write(in, "002.json", "{\"id\":2,\"text\":\"b\"}");
        write(in, "003.json", "{\"id\":3,\"text\":\"c\"}");
        write(in, "004.json", "{\"id\":4,\"text\":\"d\"}");
        write(in, "005.json", "{\"id\":5,\"text\":\"e\"}");

        AppConfig cfg = cfg(in, out, 1L, 3L, 3, 16, 32);
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, buffer);
        pipeline.awaitCompletion(20, TimeUnit.SECONDS);

        List<Path> outs = listOutJson(out);
        assertEquals(5, outs.size());
        assertEquals("000000.json", outs.get(0).getFileName().toString());
        assertEquals("000004.json", outs.get(4).getFileName().toString());

        for (int i = 0; i < 5; i++) {
            JsonNode node = JsonSupport.mapper().readTree(Files.readString(outs.get(i)));
            assertEquals(i + 1, node.get("id").asInt());
            assertNotNull(node.get("processedAt"));
            assertTrue(node.get("processedAt").asLong() > 0);
        }
        assertEquals(5, pipeline.fileWriter().getWrittenCount());
        assertFalse(pipeline.fileWriter().hasFailed());
    }

    @Test
    void brokenJsonSkipped_validKeepDenseSeq() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        write(in, "001.json", "{\"id\":1}");
        write(in, "002.json", "{broken");
        write(in, "003.json", "{\"id\":3}");
        write(in, "note.txt", "ignore");

        AppConfig cfg = cfg(in, out, 1L, 2L, 2, 8, 16);
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, buffer);
        pipeline.awaitCompletion(15, TimeUnit.SECONDS);

        List<Path> outs = listOutJson(out);
        assertEquals(2, outs.size());
        assertEquals("000000.json", outs.get(0).getFileName().toString());
        assertEquals("000001.json", outs.get(1).getFileName().toString());

        JsonNode n0 = JsonSupport.mapper().readTree(Files.readString(outs.get(0)));
        JsonNode n1 = JsonSupport.mapper().readTree(Files.readString(outs.get(1)));
        assertEquals(1, n0.get("id").asInt());
        assertEquals(3, n1.get("id").asInt());
        assertEquals(1, pipeline.fileReader().getSkippedCount());
        assertEquals(2, pipeline.fileReader().getAssignedCount());
    }

    @Test
    void requestStop_completesWithoutDeadlock() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        for (int i = 1; i <= 30; i++) {
            write(in, String.format("%03d.json", i), "{\"n\":" + i + "}");
        }

        // Slow-ish processing so stop can interrupt mid-batch
        AppConfig cfg = cfg(in, out, 10L, 30L, 2, 4, 32);
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, buffer);

        Thread stopper = new Thread(() -> {
            try {
                Thread.sleep(40);
                pipeline.requestStop();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, "stopper");
        stopper.start();

        pipeline.awaitCompletion(20, TimeUnit.SECONDS);
        stopper.join(2_000);

        assertTrue(pipeline.fileReader().isStopRequested()
                || pipeline.fileReader().getAssignedCount() <= 30);
        assertFalse(pipeline.fileWriter().hasFailed());
    }

    private static List<Path> listOutJson(Path out) throws Exception {
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
                3, 10L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }
}
