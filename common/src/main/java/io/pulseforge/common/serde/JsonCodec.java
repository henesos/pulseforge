package io.pulseforge.common.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Wire codec shared by every module, so control plane, workers and ingestor cannot drift apart in
 * how they encode the same record.
 */
public final class JsonCodec {

    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                    // Tolerate fields added by a newer peer during a rolling restart.
                    .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private JsonCodec() {
        throw new AssertionError("utility class");
    }

    public static byte[] encode(Object value) {
        try {
            return MAPPER.writeValueAsBytes(value);
        } catch (IOException e) {
            throw new SerializationException("failed to encode " + value.getClass().getName(), e);
        }
    }

    public static <T> T decode(byte[] payload, Class<T> type) {
        try {
            return MAPPER.readValue(payload, type);
        } catch (IOException e) {
            throw new SerializationException(
                    "failed to decode "
                            + type.getName()
                            + " from: "
                            + new String(payload, StandardCharsets.UTF_8),
                    e);
        }
    }

    /** Escape hatch for callers that need the configured mapper itself (e.g. YAML variants). */
    public static ObjectMapper mapper() {
        return MAPPER;
    }

    /** Thrown when a payload cannot be encoded or decoded; always carries the offending type. */
    public static class SerializationException extends RuntimeException {
        public SerializationException(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
