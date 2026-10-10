package com.renaissance.pipeline.util;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.renaissance.pipeline.model.Message;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Хелперы Jackson: parse JSON-объектов (битые/не-object — skip), сборка Message, сериализация.
 */
public final class JsonSupport {

    /**
     * Строгий разбор и запись без потери точности:
     * <ul>
     *   <li>{@code FAIL_ON_TRAILING_TOKENS} — после первого JSON-значения ничего лишнего
     *       (иначе {@code {"id":1}{"id":2}} молча терял бы второй объект);</li>
     *   <li>дроби читаются как {@code BigDecimal} и пишутся «как есть», без округления до double
     *       и без {@code E}-нотации.</li>
     * </ul>
     */
    private static final ObjectMapper MAPPER = JsonMapper.builder()
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
            .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
            .enable(JsonGenerator.Feature.WRITE_BIGDECIMAL_AS_PLAIN)
            .nodeFactory(JsonNodeFactory.withExactBigDecimals(true))
            .build();

    private JsonSupport() {
    }

    /** Общий ObjectMapper. */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /**
     * Разобрать текст как JSON-объект. Пустой/битый/не-object → {@link Optional#empty()}.
     * "{}" — валидный пустой объект.
     */
    public static Optional<ObjectNode> parseObject(String text) {
        if (text == null || text.isBlank()) {
            return Optional.empty();
        }
        try {
            JsonNode node = MAPPER.readTree(text);
            if (node != null && node.isObject()) {
                return Optional.of((ObjectNode) node);
            }
            return Optional.empty();
        } catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    /** Message из ObjectNode с deepCopy payload и заданным seq. */
    public static Message fromParsed(ObjectNode payload, long seq) {
        return new Message(seq, payload.deepCopy());
    }

    /** Message из JsonNode-объекта; иначе IllegalArgumentException. */
    public static Message fromParsed(JsonNode payload, long seq) {
        if (payload == null || !payload.isObject()) {
            throw new IllegalArgumentException("payload must be a JSON object");
        }
        return fromParsed((ObjectNode) payload, seq);
    }

    /**
     * Сериализация в UTF-8 JSON-строку.
     * Если задан {@code processedAt}, пишется в payload под ключом {@code processedAt}.
     */
    public static String toJson(Message message) {
        try {
            ObjectNode out = message.payload().deepCopy();
            if (message.processedAt() != null) {
                out.put("processedAt", message.processedAt());
            }
            return MAPPER.writeValueAsString(out);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize message seq=" + message.seq(), e);
        }
    }

    /** Записать JSON сообщения в OutputStream. */
    public static void write(Message message, OutputStream out) throws IOException {
        out.write(toJson(message).getBytes(StandardCharsets.UTF_8));
    }

    /** Записать JSON сообщения в файл. */
    public static void write(Message message, Path path) throws IOException {
        Files.writeString(path, toJson(message), StandardCharsets.UTF_8);
    }
}
