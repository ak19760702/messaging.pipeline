package com.renaissance.pipeline.stress;

import com.fasterxml.jackson.databind.JsonNode;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.pipeline.Pipeline;
import com.renaissance.pipeline.util.JsonSupport;
import com.renaissance.pipeline.writer.FileWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(value = 120, unit = TimeUnit.SECONDS)
class ChaosJsonStressIT {

    private static final int TOTAL = 300;
    private static final int BROKEN_EVERY = 5; // ~20% broken

    @TempDir
    Path tempDir;

    @Test
    void mixValidAndBroken_outOnlyValidInOrder() throws Exception {
        Path in = tempDir.resolve("In");
        Path out = tempDir.resolve("Out");
        Files.createDirectories(in);
        Files.createDirectories(out);

        List<Integer> validIds = new ArrayList<>();
        int fileIndex = 0;
        for (int i = 0; i < TOTAL; i++) {
            String name = String.format("%06d.json", fileIndex++);
            if (i % BROKEN_EVERY == 0) {
                StressSupport.writeJson(in, name, "{broken-" + i);
            } else {
                StressSupport.writeJson(in, name, "{\"id\":" + i + "}");
                validIds.add(i);
            }
        }

        AppConfig cfg = StressSupport.cfg(in, out, 1L, 3L, 3, 16, 128);
        Pipeline pipeline = StressSupport.runFull(cfg, in, out, 100);

        int expectedValid = validIds.size();
        int expectedBroken = TOTAL - expectedValid;
        assertEquals(expectedValid, pipeline.fileReader().getAssignedCount());
        assertEquals(expectedBroken, pipeline.fileReader().getSkippedCount());
        assertEquals(expectedValid, pipeline.fileWriter().getWrittenCount());

        List<Path> outs = StressSupport.listOutJson(out);
        assertEquals(expectedValid, outs.size());
        for (int s = 0; s < expectedValid; s++) {
            assertEquals(FileWriter.paddedName(s), outs.get(s).getFileName().toString());
            JsonNode n = JsonSupport.mapper().readTree(Files.readString(outs.get(s)));
            assertEquals(validIds.get(s).intValue(), n.get("id").asInt());
            assertTrue(n.has("processedAt"));
        }
    }
}
