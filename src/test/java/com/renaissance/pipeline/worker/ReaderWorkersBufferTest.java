package com.renaissance.pipeline.worker;

import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.buffer.ReorderBuffer.TakeResult;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.model.Message;
import com.renaissance.pipeline.pipeline.Pipeline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 20, unit = TimeUnit.SECONDS)
class ReaderWorkersBufferTest {

    @TempDir
    Path tempDir;

    @Test
    void readerWorkersBuffer_inOrderDrainWithSkipAndProcessedAt() throws Exception {
        // 4 valid + 1 broken; lex: 001..005
        write("001.json", "{\"id\":1}");
        write("002.json", "{not-json");
        write("003.json", "{\"id\":3}");
        write("004.json", "{\"id\":4}");
        write("005.json", "{\"id\":5}");
        write("note.txt", "ignore me");

        AppConfig cfg = cfg(1L, 3L, 2, 10, 16);
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.start(cfg, tempDir, buffer);
        pipeline.awaitCompletion(15, TimeUnit.SECONDS);

        assertEquals(4, pipeline.fileReader().getAssignedCount());
        assertEquals(1, pipeline.fileReader().getSkippedCount());

        List<TakeResult> results = drainUntilEnd(buffer);
        assertEquals(5, results.size()); // 4 Data + End
        assertInstanceOf(TakeResult.End.class, results.get(4));

        long expectedSeq = 0;
        for (int i = 0; i < 4; i++) {
            TakeResult.Data data = assertInstanceOf(TakeResult.Data.class, results.get(i));
            assertEquals(expectedSeq, data.message().seq());
            assertNotNull(data.message().processedAt());
            expectedSeq++;
        }
        assertTrue(buffer.isDrained());
    }

    @Test
    void outOfOrderWorkerCompletionStillDrainsInSeqOrder() throws Exception {
        write("001.json", "{\"id\":1}");
        write("002.json", "{\"id\":2}");
        write("003.json", "{\"id\":3}");
        write("004.json", "{\"id\":4}");
        write("005.json", "{\"id\":5}");

        // t2 slightly > t1, 3 workers → completions can finish out of order
        AppConfig cfg = cfg(1L, 5L, 3, 8, 16);
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.start(cfg, tempDir, buffer);
        pipeline.awaitCompletion(15, TimeUnit.SECONDS);

        List<Long> seqs = new ArrayList<>();
        while (true) {
            TakeResult r = buffer.take();
            if (r instanceof TakeResult.End) {
                break;
            }
            TakeResult.Data d = assertInstanceOf(TakeResult.Data.class, r);
            seqs.add(d.message().seq());
        }
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), seqs);
    }

    @Test
    void injectedWorkerFailYieldsTombstoneInOrder() throws Exception {
        write("001.json", "{\"id\":1}");
        write("002.json", "{\"id\":2}");
        write("003.json", "{\"id\":3}");

        AppConfig cfg = cfg(1L, 2L, 2, 8, 16);
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.start(cfg, tempDir, buffer, m -> m.seq() == 1L);
        pipeline.awaitCompletion(15, TimeUnit.SECONDS);

        TakeResult r0 = buffer.take();
        TakeResult r1 = buffer.take();
        TakeResult r2 = buffer.take();
        TakeResult end = buffer.take();

        assertInstanceOf(TakeResult.Data.class, r0);
        assertEquals(0L, ((TakeResult.Data) r0).message().seq());
        assertInstanceOf(TakeResult.Tombstone.class, r1);
        assertInstanceOf(TakeResult.Data.class, r2);
        assertEquals(2L, ((TakeResult.Data) r2).message().seq());
        assertInstanceOf(TakeResult.End.class, end);
    }

    @Test
    void boundedQueueBackpressureDoesNotLoseMessages() throws Exception {
        for (int i = 0; i < 6; i++) {
            write(String.format("%03d.json", i + 1), "{\"n\":" + i + "}");
        }

        // capacity=1 + slow workers → reader blocks on put; still completes
        AppConfig cfg = cfg(1L, 8L, 1, 1, 16);
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.start(cfg, tempDir, buffer);
        pipeline.awaitCompletion(20, TimeUnit.SECONDS);

        assertEquals(6, pipeline.fileReader().getAssignedCount());
        int data = 0;
        while (true) {
            TakeResult r = buffer.take();
            if (r instanceof TakeResult.End) {
                break;
            }
            assertInstanceOf(TakeResult.Data.class, r);
            data++;
        }
        assertEquals(6, data);
    }

    private List<TakeResult> drainUntilEnd(ReorderBuffer buffer) throws InterruptedException {
        List<TakeResult> results = new ArrayList<>();
        while (true) {
            TakeResult r = buffer.take();
            results.add(r);
            if (r instanceof TakeResult.End) {
                return results;
            }
        }
    }

    private void write(String name, String content) throws Exception {
        Files.writeString(tempDir.resolve(name), content, StandardCharsets.UTF_8);
    }

    private static AppConfig cfg(long t1, long t2, int workers, int queueCap, int window) {
        return new AppConfig(
                t1, t2,
                Path.of("In"), Path.of("Out"),
                queueCap, window,
                workers, workers,
                3, 50L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }
}
