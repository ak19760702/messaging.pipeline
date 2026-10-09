package com.renaissance.pipeline.worker;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.buffer.ReorderBuffer.TakeResult;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.model.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Path;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 10, unit = TimeUnit.SECONDS)
class ProcessingWorkerTest {

    @Test
    void processedMessageHasProcessedAtAndGoesToBuffer() throws Exception {
        AppConfig cfg = fastCfg();
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        ReorderBuffer buffer = new ReorderBuffer(8);

        Thread worker = new Thread(new ProcessingWorker(cfg, queue, buffer), "w");
        worker.start();

        Message msg = new Message(0L, obj("id", 1));
        queue.put(msg);
        queue.put(Message.poisonPill());

        worker.join(5000);
        assertTrue(!worker.isAlive());

        TakeResult result = buffer.take();
        TakeResult.Data data = assertInstanceOf(TakeResult.Data.class, result);
        assertEquals(0L, data.message().seq());
        assertNotNull(data.message().processedAt());
        assertTrue(data.message().processedAt() > 0L);
    }

    @Test
    void poisonPillEndsWorkerRun() throws Exception {
        AppConfig cfg = fastCfg();
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(4);
        ReorderBuffer buffer = new ReorderBuffer(4);

        Thread worker = new Thread(new ProcessingWorker(cfg, queue, buffer), "w-poison");
        worker.start();
        queue.put(Message.poisonPill());

        worker.join(3000);
        assertTrue(!worker.isAlive());
        assertEquals(0L, buffer.nextToWrite());
    }

    @Test
    void failAfterProcessPutsTombstone() throws Exception {
        AppConfig cfg = fastCfg();
        BlockingQueue<Message> queue = new ArrayBlockingQueue<>(8);
        ReorderBuffer buffer = new ReorderBuffer(8);

        ProcessingWorker worker = new ProcessingWorker(
                cfg, queue, buffer, m -> m.seq() == 0L);
        Thread t = new Thread(worker, "w-fail");
        t.start();

        queue.put(new Message(0L, obj("id", 99)));
        queue.put(Message.poisonPill());
        t.join(5000);
        assertTrue(!t.isAlive());

        buffer.signalEnd();
        assertInstanceOf(TakeResult.Tombstone.class, buffer.take());
        assertInstanceOf(TakeResult.End.class, buffer.take());
    }

    private static AppConfig fastCfg() {
        return new AppConfig(
                1L, 1L,
                Path.of("In"), Path.of("Out"),
                10, 16,
                1, 1,
                3, 50L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }

    private static ObjectNode obj(String key, int value) {
        ObjectNode n = JsonNodeFactory.instance.objectNode();
        n.put(key, value);
        return n;
    }
}
