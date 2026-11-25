package io.kafkaflow.source;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;
import java.util.List;

/**
 * One message read from a topic for the "sample message" view.
 * Key, value and headers are already decoded for display (see {@link Payload#of}).
 */
public record SampledMessage(
        String topic,
        int partition,
        long offset,
        long timestampMs,
        String timestampType,     // "CreateTime", "LogAppendTime" or "NoTimestampType"
        Payload key,
        Payload value,
        List<Header> headers,
        long earliestOffset,      // range the offset was picked from, shown for context
        long latestOffset
) {

    public record Header(String key, Payload value) {}

    /** The registry schema an Avro payload was decoded with. */
    public record SchemaInfo(int id, String name) {}

    /**
     * Bytes made readable: {@code encoding} is "null" (no bytes), "text" (valid UTF-8 without binary control
     * characters), "avro" (Confluent wire format decoded with its registry schema, {@code text} is indented JSON)
     * or "hex" (anything else). {@code size} is the full size in bytes; {@code truncated} tells whether
     * {@code text} only covers the first part of it. {@code schema} is set for "avro"; {@code problem} explains,
     * when set, why the bytes could not be decoded.
     */
    public record Payload(String encoding, String text, int size, boolean truncated, SchemaInfo schema, String problem) {

        public static final Payload NULL = new Payload("null", null, 0, false);

        public Payload(String encoding, String text, int size, boolean truncated) {
            this(encoding, text, size, truncated, null, null);
        }

        /** The same payload with an explanation of why it could not be decoded. */
        public Payload withProblem(String problem) {
            return new Payload(encoding, text, size, truncated, schema, problem);
        }

        /** Decodes up to {@code maxBytes} for display (hex output is capped at a quarter of that). */
        public static Payload of(byte[] bytes, int maxBytes) {
            if (bytes == null) return NULL;
            int limit = Math.min(bytes.length, maxBytes);
            String text = utf8(bytes, limit);
            if (text != null) return new Payload("text", text, bytes.length, limit < bytes.length);
            int hexLimit = Math.min(bytes.length, Math.max(16, maxBytes / 4));
            String hex = HexFormat.ofDelimiter(" ").formatHex(bytes, 0, hexLimit);
            return new Payload("hex", hex, bytes.length, hexLimit < bytes.length);
        }

        /** The text if the first {@code limit} bytes are readable UTF-8, else null. */
        private static String utf8(byte[] bytes, int limit) {
            if (limit == bytes.length) {
                String s = strictUtf8(bytes, limit);
                return s != null && readable(s) ? s : null;
            }
            // Truncated: the cut may fall inside a multi-byte character, so allow dropping up to 3 bytes.
            for (int cut = 0; cut <= 3 && cut < limit; cut++) {
                String s = strictUtf8(bytes, limit - cut);
                if (s != null) return readable(s) ? s : null;
            }
            return null;
        }

        private static String strictUtf8(byte[] bytes, int len) {
            try {
                return StandardCharsets.UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes, 0, len))
                        .toString();
            } catch (CharacterCodingException e) {
                return null;
            }
        }

        /** Rejects text full of control characters, which is binary that happens to be valid UTF-8. */
        private static boolean readable(String s) {
            int control = 0;
            for (int i = 0; i < s.length(); i++) {
                char c = s.charAt(i);
                if (c < 0x20 && c != '\n' && c != '\r' && c != '\t') control++;
            }
            // A leading control byte is a binary format marker (e.g. the 0x00 of Confluent Avro/Protobuf).
            return control == 0 || (s.charAt(0) >= 0x20 && control * 20 < s.length());
        }
    }
}
