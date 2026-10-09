package com.renaissance.pipeline.reader;

import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class FileReaderEdgeTest {

    @TempDir
    Path tempDir;

    @Test
    void emptyIn_assignsZeroAndSendsPoison() throws Exception {
        Files.createDirectories(tempDir);
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 2);
        reader.run();

        assertEquals(0, reader.getAssignedCount());
        assertEquals(0, reader.getSkippedCount());
        assertTrue(queue.take().isPoison());
        assertTrue(queue.take().isPoison());
        assertTrue(queue.isEmpty());
    }

    @Test
    void disappearedAfterList_skipsAndContinues() throws Exception {
        Path victim = tempDir.resolve("001.json");
        Files.writeString(victim, "{\"id\":1}", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("002.json"), "{\"id\":2}", StandardCharsets.UTF_8);

        List<Path> listed = FileReader.listJsonFilesLexSorted(tempDir);
        assertEquals(2, listed.size());
        Files.delete(victim);

        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 0);
        reader.processListedFiles(listed);

        assertEquals(1, reader.getAssignedCount());
        assertEquals(1, reader.getSkippedCount());
        Message m = queue.poll(1, TimeUnit.SECONDS);
        assertEquals(0L, m.seq());
        assertEquals(2, m.payload().get("id").asInt());
    }

    @Test
    void nonJsonIgnored_mixedWithValid() throws Exception {
        Files.writeString(tempDir.resolve("001.json"), "{\"id\":1}", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("readme.txt"), "nope", StandardCharsets.UTF_8);
        Files.writeString(tempDir.resolve("002.json"), "{\"id\":2}", StandardCharsets.UTF_8);

        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        FileReader reader = new FileReader(fastCfg(), tempDir, queue, 0);
        reader.run();

        assertEquals(2, reader.getAssignedCount());
        assertEquals(0, reader.getSkippedCount());
    }

    private static AppConfig fastCfg() {
        return new AppConfig(
                0L, 1L,
                Path.of("In"), Path.of("Out"),
                16, 16,
                1, 1,
                1, 1L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }
}
