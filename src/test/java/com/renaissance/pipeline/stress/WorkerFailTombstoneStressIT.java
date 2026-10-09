package com.renaissance.pipeline.stress;

import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.pipeline.Pipeline;
import com.renaissance.pipeline.writer.FileWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120, unit = TimeUnit.SECONDS)
class WorkerFailTombstoneStressIT {

    private static final int N = 120;
    private static final int FAIL_EVERY = 7;

    @TempDir
    Path tempDir;

    @Test
    void periodicWorkerFails_tombstonesSkipOutWithoutHang() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);
        StressSupport.generateValid(in, N);

        AppConfig cfg = StressSupport.cfg(in, out, 1L, 3L, 4, 16, 64);
        Pipeline pipeline = StressSupport.runFull(
                cfg, in, out, 100, m -> m.seq() % FAIL_EVERY == 0);

        long failCount = 0;
        for (long seq = 0; seq < N; seq++) {
            if (seq % FAIL_EVERY == 0) {
                failCount++;
            }
        }
        long expectedWritten = N - failCount;

        assertEquals(N, pipeline.fileReader().getAssignedCount());
        assertEquals(expectedWritten, pipeline.fileWriter().getWrittenCount());
        assertEquals(failCount, pipeline.fileWriter().getSkippedCount());

        List<Path> outs = StressSupport.listOutJson(out);
        assertEquals(expectedWritten, outs.size());

        for (long seq = 0; seq < N; seq++) {
            Path f = out.resolve(FileWriter.paddedName(seq));
            if (seq % FAIL_EVERY == 0) {
                assertFalse(Files.exists(f), "tombstone seq should not write " + seq);
            } else {
                assertTrue(Files.exists(f), "expected Out for seq " + seq);
            }
        }
    }
}
