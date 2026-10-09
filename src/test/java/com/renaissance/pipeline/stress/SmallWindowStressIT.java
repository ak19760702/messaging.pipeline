package com.renaissance.pipeline.stress;

import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.pipeline.Pipeline;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(value = 120, unit = TimeUnit.SECONDS)
class SmallWindowStressIT {

    private static final int N = 250;

    @TempDir
    Path tempDir;

    @Test
    void smallReorderWindow_highContention_stillCorrect() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);
        StressSupport.generateValid(in, N);

        // window=8 << N, 5 workers → strong ring backpressure
        AppConfig cfg = StressSupport.cfg(in, out, 1L, 4L, 5, 8, 8);
        Pipeline pipeline = StressSupport.runFull(cfg, in, out, 100);

        assertEquals(N, pipeline.fileReader().getAssignedCount());
        assertEquals(N, pipeline.fileWriter().getWrittenCount());
        StressSupport.assertOutOrderAndIds(out, N);
    }
}
