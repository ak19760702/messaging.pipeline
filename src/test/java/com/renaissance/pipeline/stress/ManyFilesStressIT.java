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

@Timeout(value = 180, unit = TimeUnit.SECONDS)
class ManyFilesStressIT {

    private static final int N = 800;

    @TempDir
    Path tempDir;

    @Test
    void manyValidFiles_preserveOrder() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);
        StressSupport.generateValid(in, N);

        // t2 > t1, workers auto ≈ 3
        AppConfig cfg = StressSupport.cfg(in, out, 1L, 3L, 0, 64, 256);
        Pipeline pipeline = StressSupport.runFull(cfg, in, out, 150);

        assertEquals(N, pipeline.fileReader().getAssignedCount());
        assertEquals(N, pipeline.fileWriter().getWrittenCount());
        StressSupport.assertOutOrderAndIds(out, N);
    }
}
