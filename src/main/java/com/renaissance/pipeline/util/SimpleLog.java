package com.renaissance.pipeline.util;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Objects;

/**
 * Простой текстовый лог: префикс {@code YYYYMMDDHHMMSS_} + сообщение в консоль и файл.
 * Имя файла при конфигурации тоже получает тот же префикс (один на запуск).
 * Без уровней. Потокобезопасный append.
 */
public final class SimpleLog {

    public static final String DEFAULT_LOG_FILE = "logs/pipeline.log";

    /** Префикс времени для строк и имён файлов: {@code 20261008200600_}. */
    public static final DateTimeFormatter TS_PREFIX =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static final Object LOCK = new Object();

    private static volatile Path logFile = resolveDefault();

    private SimpleLog() {
    }

    /**
     * Добавить к имени файла префикс {@code YYYYMMDDHHMMSS_}
     * ({@code logs/pipeline.log} → {@code logs/20261008200600_pipeline.log}).
     */
    public static Path withTimestampPrefix(Path path) {
        Objects.requireNonNull(path, "path");
        Path normalized = path.normalize();
        String name = normalized.getFileName() == null
                ? DEFAULT_LOG_FILE
                : normalized.getFileName().toString();
        String stamped = LocalDateTime.now().format(TS_PREFIX) + "_" + name;
        Path parent = normalized.getParent();
        return parent == null ? Path.of(stamped) : parent.resolve(stamped);
    }

    /** Задать файл лога (относительные пути — от {@code user.dir}). */
    public static void configure(Path path) {
        Objects.requireNonNull(path, "path");
        synchronized (LOCK) {
            logFile = path.isAbsolute()
                    ? path.normalize()
                    : Path.of(System.getProperty("user.dir")).resolve(path).normalize();
        }
    }

    /** Текущий путь к файлу лога. */
    public static Path logFile() {
        return logFile;
    }

    /** Записать строку в консоль и файл. */
    public static void log(String message) {
        String line = prefix() + (message == null ? "" : message);
        synchronized (LOCK) {
            System.out.println(line);
            appendToFile(line);
        }
    }

    /** Как {@link #log(String)}, плюс stack trace throwable. */
    public static void log(String message, Throwable t) {
        if (t == null) {
            log(message);
            return;
        }
        StringWriter sw = new StringWriter();
        t.printStackTrace(new PrintWriter(sw));
        String stack = sw.toString().stripTrailing();
        String body = (message == null ? "" : message);
        if (!body.isEmpty() && !stack.isEmpty()) {
            body = body + System.lineSeparator() + stack;
        } else if (!stack.isEmpty()) {
            body = stack;
        }
        String[] parts = body.split("\\R", -1);
        synchronized (LOCK) {
            for (String part : parts) {
                String line = prefix() + part;
                System.out.println(line);
                appendToFile(line);
            }
        }
    }

    /** Префикс текущей секунды: {@code YYYYMMDDHHMMSS_}. */
    private static String prefix() {
        return LocalDateTime.now().format(TS_PREFIX) + "_";
    }

    /** Дописать строку в файл лога (создаёт родительские каталоги). */
    private static void appendToFile(String line) {
        Path path = logFile;
        try {
            Path parent = path.getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Files.writeString(
                    path,
                    line + System.lineSeparator(),
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println(prefix()
                    + "failed to write log file " + path + ": " + e.getMessage());
        }
    }

    private static Path resolveDefault() {
        return Path.of(System.getProperty("user.dir")).resolve(DEFAULT_LOG_FILE).normalize();
    }
}
