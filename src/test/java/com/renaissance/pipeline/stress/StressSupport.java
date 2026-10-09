package com.renaissance.pipeline.stress;

import com.fasterxml.jackson.databind.JsonNode;
import com.renaissance.pipeline.buffer.ReorderBuffer;
import com.renaissance.pipeline.config.AppConfig;
import com.renaissance.pipeline.pipeline.Pipeline;
import com.renaissance.pipeline.util.JsonSupport;
import com.renaissance.pipeline.writer.FileWriter;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Stream;

import com.renaissance.pipeline.model.Message;

final class StressSupport {

    private StressSupport() {
    }

    static AppConfig cfg(
            Path in, Path out, long t1, long t2, int workers, int queueCap, int window) {
        return new AppConfig(
                t1, t2,
                in, out,
                queueCap, window,
                workers, workers > 0 ? workers : Math.max(1, (int) Math.ceil((double) t2 / t1)),
                2, 5L, Path.of(AppConfig.DEFAULT_LOG_FILE));
    }

    static void writeJson(Path dir, String name, String content) throws Exception {
        Files.writeString(dir.resolve(name), content, StandardCharsets.UTF_8);
    }

    static void generateValid(Path in, int count) throws Exception {
        for (int i = 0; i < count; i++) {
            writeJson(in, String.format("%06d.json", i), "{\"id\":" + i + "}");
        }
    }

    static List<Path> listOutJson(Path out) throws Exception {
        if (!Files.isDirectory(out)) {
            return List.of();
        }
        try (Stream<Path> s = Files.list(out)) {
            List<Path> list = new ArrayList<>();
            s.filter(p -> p.getFileName().toString().endsWith(".json"))
                    .sorted(Comparator.comparing(p -> p.getFileName().toString()))
                    .forEach(list::add);
            return list;
        }
    }

    static Pipeline runFull(
            AppConfig cfg, Path in, Path out, long timeoutSec) throws Exception {
        return runFull(cfg, in, out, timeoutSec, m -> false);
    }

    static Pipeline runFull(
            AppConfig cfg,
            Path in,
            Path out,
            long timeoutSec,
            Predicate<Message> failAfterProcess) throws Exception {
        ReorderBuffer buffer = new ReorderBuffer(cfg.reorderWindow());
        Pipeline pipeline = Pipeline.startFull(cfg, in, out, buffer, failAfterProcess);
        pipeline.awaitCompletion(timeoutSec, TimeUnit.SECONDS);
        return pipeline;
    }

    static void assertOutOrderAndIds(Path out, int expectedCount) throws Exception {
        List<Path> outs = listOutJson(out);
        if (outs.size() != expectedCount) {
            throw new AssertionError("Out size " + outs.size() + " != " + expectedCount);
        }
        for (int i = 0; i < expectedCount; i++) {
            String expectedName = FileWriter.paddedName(i);
            if (!expectedName.equals(outs.get(i).getFileName().toString())) {
                throw new AssertionError("name at " + i + ": " + outs.get(i).getFileName()
                        + " expected " + expectedName);
            }
            JsonNode n = JsonSupport.mapper().readTree(Files.readString(outs.get(i)));
            if (!n.has("processedAt")) {
                throw new AssertionError("missing processedAt in " + expectedName);
            }
            if (n.get("id").asInt() != i) {
                throw new AssertionError("id mismatch at seq " + i);
            }
        }
    }
}
