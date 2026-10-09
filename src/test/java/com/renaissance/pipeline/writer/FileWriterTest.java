package com.renaissance.pipeline.writer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.model.Message;
import com.renaissance.pipeline.util.JsonSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 15, unit = TimeUnit.SECONDS)
class FileWriterTest {

    @TempDir
    Path tempDir;

    @Test
    void writesPaddedFilesForDataInOrder() throws Exception {
        Path out = tempDir.resolve("Out");
        Files.createDirectories(out);
        AppConfig cfg = cfg(1L);
        ReorderBuffer buffer = new ReorderBuffer(8);
        FileWriter writer = new FileWriter(cfg, out, buffer);
        Thread t = new Thread(writer, "test-writer");
        t.start();

        buffer.put(0, msg(0, 1));
        buffer.put(1, msg(1, 2));
        buffer.signalEnd();
        t.join(10_000);
        assertFalse(t.isAlive());

        assertTrue(Files.isRegularFile(out.resolve("000000.json")));
        assertTrue(Files.isRegularFile(out.resolve("000001.json")));
        assertEquals(2, writer.getWrittenCount());
        assertEquals(0, writer.getSkippedCount());

        JsonNode n0 = JsonSupport.mapper().readTree(Files.readString(out.resolve("000000.json")));
        assertEquals(1, n0.get("id").asInt());
        assertTrue(n0.has("processedAt"));
        assertEquals(1_000L, n0.get("processedAt").asLong());
    }

    @Test
    void tombstoneSkippedNoFileCreated() throws Exception {
        Path out = tempDir.resolve("Out");
        Files.createDirectories(out);
        AppConfig cfg = cfg(0L);
        ReorderBuffer buffer = new ReorderBuffer(8);
        FileWriter writer = new FileWriter(cfg, out, buffer);
        Thread t = new Thread(writer, "test-writer");
        t.start();

        buffer.putTombstone(0);
        buffer.put(1, msg(1, 99));
        buffer.signalEnd();
        t.join(10_000);
        assertFalse(t.isAlive());

        assertFalse(Files.exists(out.resolve("000000.json")));
        assertTrue(Files.isRegularFile(out.resolve("000001.json")));
        assertEquals(1, writer.getWrittenCount());
        assertEquals(1, writer.getSkippedCount());
    }

    private static Message msg(long seq, int id) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("id", id);
        Message m = new Message(seq, payload);
        m.setProcessedAt(1_000L);
        return m;
    }

    private static AppConfig cfg(long t1) {
        return new AppConfig(
                t1, 3L,
                Path.of("In"), Path.of("Out"),
                10, 8,
                2, 2,
                3, 50L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }
}
