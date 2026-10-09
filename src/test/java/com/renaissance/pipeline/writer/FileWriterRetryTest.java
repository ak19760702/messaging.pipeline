package com.renaissance.pipeline.writer;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.model.Message;
import com.renaissance.pipeline.util.JsonSupport;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 15, unit = TimeUnit.SECONDS)
class FileWriterRetryTest {

    @TempDir
    Path tempDir;

    @Test
    void retriesThenSucceeds() throws Exception {
        Path out = tempDir.resolve("Out");
        Files.createDirectories(out);
        AtomicInteger attempts = new AtomicInteger();
        FileWriter.MessageSink sink = (msg, target) -> {
            int n = attempts.incrementAndGet();
            if (n <= 2) {
                throw new IOException("injected fail #" + n);
            }
            JsonSupport.write(msg, target);
        };

        // retryCount=2 → up to 3 attempts; first two fail, third ok
        AppConfig cfg = cfg(0L, 2, 1L);
        ReorderBuffer buffer = new ReorderBuffer(4);
        FileWriter writer = new FileWriter(cfg, out, buffer, sink);
        Thread t = new Thread(writer, "retry-writer");
        t.start();

        buffer.put(0, msg(0));
        buffer.signalEnd();
        t.join(10_000);

        assertFalse(t.isAlive());
        assertFalse(writer.hasFailed());
        assertEquals(1, writer.getWrittenCount());
        assertEquals(3, attempts.get());
        assertTrue(Files.isRegularFile(out.resolve("000000.json")));
    }

    @Test
    void exhaustsRetriesAndStops() throws Exception {
        Path out = tempDir.resolve("Out");
        Files.createDirectories(out);
        AtomicInteger attempts = new AtomicInteger();
        FileWriter.MessageSink sink = (msg, target) -> {
            attempts.incrementAndGet();
            throw new IOException("always fail");
        };

        // retryCount=1 → 2 attempts then fatal
        AppConfig cfg = cfg(0L, 1, 1L);
        ReorderBuffer buffer = new ReorderBuffer(4);
        FileWriter writer = new FileWriter(cfg, out, buffer, sink);
        Thread t = new Thread(writer, "fatal-writer");
        t.start();

        buffer.put(0, msg(0));
        // Do not signalEnd yet — writer should fail mid-stream
        t.join(10_000);

        assertFalse(t.isAlive());
        assertTrue(writer.hasFailed());
        assertEquals(0, writer.getWrittenCount());
        assertEquals(2, attempts.get());
        assertFalse(Files.exists(out.resolve("000000.json")));
    }

    private static Message msg(long seq) {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("id", seq);
        Message m = new Message(seq, payload);
        m.setProcessedAt(42L);
        return m;
    }

    private static AppConfig cfg(long t1, int retryCount, long retryDelay) {
        return new AppConfig(
                t1, 3L,
                Path.of("In"), Path.of("Out"),
                8, 4,
                1, 1,
                retryCount, retryDelay, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }
}
