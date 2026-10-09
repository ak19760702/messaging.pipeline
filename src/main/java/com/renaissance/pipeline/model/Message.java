package com.renaissance.pipeline.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * Доменное сообщение: seq назначает FileReader; processedAt — epoch millis, null до обработки.
 * Poison-pill: {@link #POISON_SEQ} ({@code -1}), см. {@link #poisonPill()}.
 */
public final class Message {

    /** Sentinel seq для poison-pill (не реальный seq Reader'а). */
    public static final long POISON_SEQ = -1L;

    private final long seq;
    private final ObjectNode payload;
    private Long processedAt;

    /** Сообщение без processedAt. */
    public Message(long seq, ObjectNode payload) {
        this(seq, payload, null);
    }

    /** Сообщение с опциональным processedAt. */
    public Message(long seq, ObjectNode payload, Long processedAt) {
        if (payload == null) {
            throw new IllegalArgumentException("payload must not be null");
        }
        this.seq = seq;
        this.payload = payload;
        this.processedAt = processedAt;
    }

    /** Маркер конца потока для worker'ов; в ReorderBuffer не кладётся. */
    public static Message poisonPill() {
        return new Message(POISON_SEQ, JsonNodeFactory.instance.objectNode());
    }

    /** Является ли сообщение poison-pill. */
    public boolean isPoison() {
        return seq == POISON_SEQ;
    }

    /** Порядковый номер seq. */
    public long seq() {
        return seq;
    }

    /** Изменяемый JSON-payload (общий с сериализацией). */
    public ObjectNode payload() {
        return payload;
    }

    /** Epoch millis завершения обработки; null до установки worker'ом. */
    public Long processedAt() {
        return processedAt;
    }

    /** Установить время завершения обработки. */
    public void setProcessedAt(Long processedAt) {
        this.processedAt = processedAt;
    }

    /** Payload как JsonNode. */
    public JsonNode payloadAsJsonNode() {
        return payload;
    }
}
