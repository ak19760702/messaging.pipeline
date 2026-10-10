package com.renaissance.pipeline.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.renaissance.pipeline.model.Message;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonSupportTest {

    @Test
    void parseValidObject() {
        Optional<ObjectNode> parsed = JsonSupport.parseObject("{\"id\":1,\"text\":\"a\"}");
        assertTrue(parsed.isPresent());
        assertEquals(1, parsed.get().get("id").asInt());
        assertEquals("a", parsed.get().get("text").asText());
    }

    @Test
    void roundTripParseMessageToJson() {
        ObjectNode node = JsonSupport.parseObject("{\"id\":1,\"text\":\"a\"}").orElseThrow();
        Message msg = JsonSupport.fromParsed(node, 0L);
        String json = JsonSupport.toJson(msg);

        JsonNode again = jsonSilent(json);
        assertEquals(1, again.get("id").asInt());
        assertEquals("a", again.get("text").asText());
        assertFalse(again.has("processedAt"));
    }

    @Test
    void emptyObjectIsValidObjectNotSkip() {
        Optional<ObjectNode> parsed = JsonSupport.parseObject("{}");
        assertTrue(parsed.isPresent());
        assertEquals(0, parsed.get().size());
    }

    @Test
    void emptyStringNullArrayScalarAreSkip() {
        assertTrue(JsonSupport.parseObject("").isEmpty());
        assertTrue(JsonSupport.parseObject("null").isEmpty());
        assertTrue(JsonSupport.parseObject("[]").isEmpty());
        assertTrue(JsonSupport.parseObject("1").isEmpty());
    }

    @Test
    void brokenJsonReturnsEmptyWithoutThrowing() {
        Optional<ObjectNode> parsed = JsonSupport.parseObject("{id");
        assertTrue(parsed.isEmpty());
    }

    @Test
    void nonObjectReturnsEmpty() {
        assertTrue(JsonSupport.parseObject("[]").isEmpty());
        assertTrue(JsonSupport.parseObject("\"x\"").isEmpty());
    }

    @Test
    void processedAtSerializesIntoJson() {
        ObjectNode node = JsonSupport.parseObject("{\"id\":1}").orElseThrow();
        Message msg = JsonSupport.fromParsed(node, 42L);
        msg.setProcessedAt(1_700_000_000_000L);

        String json = JsonSupport.toJson(msg);
        JsonNode again = jsonSilent(json);
        assertEquals(1, again.get("id").asInt());
        assertEquals(1_700_000_000_000L, again.get("processedAt").asLong());
        assertEquals(42L, msg.seq());
    }

    @Test
    void secondRootValueAfterObjectIsSkip() {
        assertTrue(JsonSupport.parseObject("{\"id\":1}{\"id\":2}").isEmpty());
        assertTrue(JsonSupport.parseObject("{\"id\":1}\n{\"id\":2}").isEmpty());
        assertTrue(JsonSupport.parseObject("{\"id\":1} true").isEmpty());
    }

    @Test
    void trailingWhitespaceAfterObjectIsStillValid() {
        assertTrue(JsonSupport.parseObject("{\"id\":1}\n").isPresent());
        assertTrue(JsonSupport.parseObject("{\"id\":1}  \r\n").isPresent());
    }

    @Test
    void longDecimalsSurviveRoundTripUnchanged() {
        String in = "{\"a\":1.234567890123456789,\"b\":123456789.123456789123456789,"
                + "\"c\":0.30000000000000000000001}";
        ObjectNode node = JsonSupport.parseObject(in).orElseThrow();
        String json = JsonSupport.toJson(JsonSupport.fromParsed(node, 0L));

        assertTrue(json.contains("\"a\":1.234567890123456789"), json);
        assertTrue(json.contains("\"b\":123456789.123456789123456789"), json);
        assertTrue(json.contains("\"c\":0.30000000000000000000001"), json);
    }

    private static JsonNode jsonSilent(String json) {
        try {
            return JsonSupport.mapper().readTree(json);
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }
}
