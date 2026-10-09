package com.renaissance.pipeline.reader;

import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class FileReaderTest {

    @TempDir
    Path tempDir;

    @Test
    void validFilesGetSeqStartingAtZeroInLexOrder() throws Exception {
        // Lex order: 001 before 002. Note: without zero-pad, 1.json / 10.json / 2.json
        // would sort as 1, 10, 2 (string compare), not numeric order.
        write(tempDir.resolve("001.json"), "{\"id\":1}");
        write(tempDir.resolve("002.json"), "{\"id\":2}");

        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(16);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 0);
        reader.run();

        assertEquals(2, reader.getAssignedCount());
        assertEquals(0, reader.getSkippedCount());

        Message m0 = queue.poll(1, TimeUnit.SECONDS);
        Message m1 = queue.poll(1, TimeUnit.SECONDS);
        assertEquals(0L, m0.seq());
        assertEquals(1, m0.payload().get("id").asInt());
        assertEquals(1L, m1.seq());
        assertEquals(2, m1.payload().get("id").asInt());
        assertTrue(queue.isEmpty());
    }

    @Test
    void lexOrderABeforeB() throws Exception {
        write(tempDir.resolve("b.json"), "{\"name\":\"b\"}");
        write(tempDir.resolve("a.json"), "{\"name\":\"a\"}");

        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 0);
        reader.run();

        assertEquals("a", queue.take().payload().get("name").asText());
        assertEquals("b", queue.take().payload().get("name").asText());
    }

    @Test
    void brokenEmptyAndArrayJsonAreSkippedWithoutSeq() throws Exception {
        write(tempDir.resolve("001.json"), "{\"ok\":true}");
        write(tempDir.resolve("002.json"), "{broken");
        write(tempDir.resolve("003.json"), "");
        write(tempDir.resolve("004.json"), "[]");
        write(tempDir.resolve("005.json"), "{\"ok\":2}");

        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(16);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 0);
        reader.run();

        assertEquals(2, reader.getAssignedCount());
        assertEquals(3, reader.getSkippedCount());
        assertEquals(2, queue.size());
        assertEquals(0L, queue.take().seq());
        assertEquals(1L, queue.take().seq());
        assertTrue(queue.isEmpty());
    }

    @Test
    void nonJsonFilesAreIgnored() throws Exception {
        write(tempDir.resolve("note.txt"), "not json");
        write(tempDir.resolve("data.json"), "{\"x\":1}");
        Files.writeString(tempDir.resolve("readme.md"), "# hi", StandardCharsets.UTF_8);

        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 0);
        reader.run();

        assertEquals(1, reader.getAssignedCount());
        assertEquals(0, reader.getSkippedCount());
        assertEquals(1, queue.size());
        assertEquals(0L, queue.take().seq());
    }

    @Test
    void sendsPoisonPillsEqualToWorkerCount() throws Exception {
        write(tempDir.resolve("a.json"), "{\"a\":1}");

        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(16);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 3);
        reader.run();

        List<Message> drained = new ArrayList<>();
        queue.drainTo(drained);
        assertEquals(4, drained.size()); // 1 data + 3 poison
        assertFalse(drained.get(0).isPoison());
        assertEquals(0L, drained.get(0).seq());
        assertTrue(drained.get(1).isPoison());
        assertTrue(drained.get(2).isPoison());
        assertTrue(drained.get(3).isPoison());
    }

    @Test
    void emptyInDirOnlySendsPoisonPills() throws Exception {
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 2);
        reader.run();

        assertEquals(0, reader.getAssignedCount());
        assertTrue(queue.take().isPoison());
        assertTrue(queue.take().isPoison());
        assertTrue(queue.isEmpty());
    }

    private static AppConfig fastCfg() {
        return new AppConfig(
                1L, 3L,
                Path.of("In"), Path.of("Out"),
                10, 16,
                2, 2,
                3, 50L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }

    private static void write(Path path, String content) throws Exception {
        Files.writeString(path, content, StandardCharsets.UTF_8);
    }
}
