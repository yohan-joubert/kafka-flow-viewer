package io.kafkaflow.source;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.StreamWriteFeature;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.kafkaflow.source.SampledMessage.Payload;
import io.kafkaflow.source.SampledMessage.SchemaInfo;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Writer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.LongSupplier;

/**
 * Decodes keys and values written in the Confluent wire format (magic byte 0, 4-byte schema id, Avro body) with
 * the writer schema fetched from a Schema Registry, and renders them as indented JSON. Anything else, and anything
 * that fails to decode, is left to {@link Payload#of}, with {@link Payload#problem()} saying why.
 *
 * <p>Failed lookups are remembered so sampling stays fast: an id the registry doesn't have (or that is not Avro)
 * is not asked again for {@link #MISSING_TTL_MS}, and after a registry error (unreachable, not allowed) no id is
 * asked for {@link #ERROR_TTL_MS}. Thread-safe.
 */
public final class AvroDecoder implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(AvroDecoder.class);

    /** Finds a schema by its registry id, e.g. {@code SchemaRegistryClient::getSchemaById}. */
    @FunctionalInterface
    public interface SchemaLookup {
        ParsedSchema schemaById(int id) throws Exception;
    }

    static final long MISSING_TTL_MS = 5 * 60_000;
    static final long ERROR_TTL_MS = 30_000;

    static {
        // Corrupt or misdetected bytes can announce huge strings or collections: fail fast instead of allocating them.
        // Only applies if set before Avro reads them, and never overrides an explicit setting.
        setIfAbsent("org.apache.avro.limits.string.maxLength", String.valueOf(64 << 20));
        setIfAbsent("org.apache.avro.limits.bytes.maxLength", String.valueOf(64 << 20));
        setIfAbsent("org.apache.avro.limits.collectionItems.maxLength", String.valueOf(10_000_000));
    }

    private static final JsonFactory JSON = JsonFactory.builder().enable(StreamWriteFeature.WRITE_BIGDECIMAL_AS_PLAIN).build();
    private static final int HEADER = 5;

    private record Failure(String problem, long until) {}

    private final SchemaLookup lookup;
    private final String setupProblem;      // set when the registry client could not be created
    private final AutoCloseable resource;
    private final LongSupplier clock;
    private final Map<Integer, Schema> schemas = new ConcurrentHashMap<>();
    private final Map<Integer, Failure> missing = new ConcurrentHashMap<>();
    private volatile Failure registryError;

    public AvroDecoder(SchemaLookup lookup) {
        this(lookup, System::currentTimeMillis);
    }

    /** For tests: {@code clock} gives the current time in ms, used to expire remembered failures. */
    AvroDecoder(SchemaLookup lookup, LongSupplier clock) {
        this(lookup, null, null, clock);
    }

    private AvroDecoder(SchemaLookup lookup, String setupProblem, AutoCloseable resource, LongSupplier clock) {
        this.lookup = lookup;
        this.setupProblem = setupProblem;
        this.resource = resource;
        this.clock = clock;
    }

    /** A decoder for when no registry is configured: Avro stays hex, with a hint on how to decode it. */
    public static AvroDecoder disabled() {
        return new AvroDecoder(null, null, null, System::currentTimeMillis);
    }

    /**
     * A decoder backed by a real registry. {@code urls} is comma separated; {@code config} goes to the client.
     * Never throws: invalid settings (e.g. a missing truststore) are logged and reported with every Avro payload,
     * since decoding is optional and must not stop the viewer.
     */
    public static AvroDecoder forRegistry(String urls, Map<String, Object> config) {
        try {
            RestService rest = new RestService(List.of(urls.trim().split("\\s*,\\s*")));
            CachedSchemaRegistryClient client = new CachedSchemaRegistryClient(rest, 1000, config); // configures rest
            SchemaLookup lookup = id -> {
                try {
                    return client.getSchemaById(id);
                } catch (RestClientException | IOException e) {
                    throw e;
                } catch (RuntimeException e) {
                    // The client only parses Avro: another schema type fails with an unhelpful exception.
                    String type = rest.getId(id).getSchemaType();
                    if (type != null && !type.equals("AVRO")) throw notAvro(id, type);
                    throw e;
                }
            };
            return new AvroDecoder(lookup, null, client, System::currentTimeMillis);
        } catch (Exception e) {
            String problem = "The Schema Registry settings are invalid, so Avro is not decoded: " + redact(rootMessage(e));
            log.warn(problem);
            return new AvroDecoder(null, problem, null, System::currentTimeMillis);
        }
    }

    public Payload decode(byte[] bytes, int maxBytes) {
        Payload plain = Payload.of(bytes, maxBytes);
        if (bytes == null || bytes.length < HEADER || bytes[0] != 0) return plain;
        int id = ByteBuffer.wrap(bytes, 1, 4).getInt();
        if (id <= 0) return plain; // ids start at 1: this is something else, e.g. a long key

        if (lookup == null) {
            return plain.withProblem(setupProblem != null ? setupProblem
                    : "This looks like Avro in the Confluent format (schema id " + id + "). Set registry.url to decode it.");
        }
        Schema schema;
        try {
            schema = schema(id);
        } catch (LookupFailed e) {
            return plain.withProblem(e.getMessage());
        }

        try {
            BinaryDecoder in = DecoderFactory.get().binaryDecoder(bytes, HEADER, bytes.length - HEADER, null);
            Object datum = new GenericDatumReader<>(schema, schema, GenericData.get()).read(null, in);
            if (!in.isEnd()) throw new IOException("bytes left over after the end of the message");
            Rendered r = render(datum, schema, maxBytes);
            return new Payload("avro", r.text(), bytes.length, r.truncated(), new SchemaInfo(id, schema.getFullName()), null);
        } catch (Exception | StackOverflowError e) {
            return plain.withProblem("The bytes do not match schema id " + id + " (" + schema.getFullName() + "): "
                    + (e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage()));
        }
    }

    /** The Avro schema for an id, or LookupFailed with the reason to show (possibly remembered from before). */
    private Schema schema(int id) throws LookupFailed {
        Schema cached = schemas.get(id);
        if (cached != null) return cached;
        long now = clock.getAsLong();
        Failure known = missing.get(id);
        if (known != null && now < known.until()) throw new LookupFailed(known.problem());
        Failure down = registryError;
        if (down != null && now < down.until()) throw new LookupFailed(down.problem());

        try {
            ParsedSchema parsed = lookup.schemaById(id);
            if (parsed == null) throw new IllegalStateException("the registry returned no schema");
            if (!"AVRO".equals(parsed.schemaType()) || !(parsed.rawSchema() instanceof Schema avro)) {
                throw notAvro(id, parsed.schemaType());
            }
            schemas.put(id, avro);
            missing.remove(id);
            registryError = null;
            return avro;
        } catch (NotAvro e) {
            missing.put(id, new Failure(e.getMessage(), now + MISSING_TTL_MS));
            throw new LookupFailed(e.getMessage());
        } catch (Exception e) {
            String problem = describeLookupError(id, e);
            boolean aboutThisId = e instanceof RestClientException r
                    && r.getStatus() != 401 && r.getStatus() != 403 && r.getStatus() < 500;
            if (aboutThisId) missing.put(id, new Failure(problem, now + MISSING_TTL_MS));
            else registryError = new Failure(problem, now + ERROR_TTL_MS); // the registry itself: every id
            throw new LookupFailed(problem);
        }
    }

    private static NotAvro notAvro(int id, String type) {
        return new NotAvro("Schema id " + id + " is a " + type + " schema. Only Avro is decoded, so the bytes are shown as hex.");
    }

    private static String describeLookupError(int id, Exception e) {
        if (e instanceof RestClientException r) {
            int status = r.getStatus();
            if (status == 404) return "Schema id " + id + " was not found in the registry.";
            if (status == 401 || status == 403) {
                return "Not allowed to read schema id " + id + " from the registry (HTTP " + status
                        + "). Check the registry.* credentials.";
            }
            return "The registry answered HTTP " + status + " for schema id " + id + ": " + redact(r.getMessage());
        }
        String m = e.getMessage();
        return "Cannot fetch schema id " + id + " from the registry: "
                + e.getClass().getSimpleName() + (m == null ? "" : ": " + redact(m));
    }

    private static String rootMessage(Throwable e) {
        Throwable t = e;
        while (t.getCause() != null && t.getCause() != t) t = t.getCause();
        return t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage();
    }

    /** Removes credentials embedded in URLs (https://user:password@host). */
    private static String redact(String s) {
        return s == null ? "" : s.replaceAll("(?<=://)[^/@\\s]+@", "***@");
    }

    // ---- rendering -----------------------------------------------------------------------

    /** Rendered JSON, cut at the byte limit; {@code charsProduced} is how much was generated before stopping. */
    record Rendered(String text, boolean truncated, long charsProduced) {}

    /**
     * Renders a datum as indented JSON (a plain top-level string as itself, without quotes). Generation stops soon
     * after {@code maxBytes}: a few bytes of Avro can describe millions of items.
     */
    static Rendered render(Object datum, Schema schema, int maxBytes) {
        String text;
        long produced;
        if (datum != null && unionBranch(datum, schema).getType() == Schema.Type.STRING) {
            text = datum.toString();
            produced = text.length();
        } else {
            BudgetWriter out = new BudgetWriter(maxBytes);
            try (JsonGenerator g = JSON.createGenerator(out)) {
                g.useDefaultPrettyPrinter();
                write(g, datum, schema);
            } catch (BudgetWriter.Full full) {
                // enough for display
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            text = out.text();
            produced = out.produced();
        }
        byte[] utf8 = text.getBytes(StandardCharsets.UTF_8);
        boolean truncated = utf8.length > maxBytes;
        return new Rendered(truncated ? cut(utf8, maxBytes) : text, truncated, produced);
    }

    private static Schema unionBranch(Object v, Schema s) {
        return s.getType() == Schema.Type.UNION ? s.getTypes().get(GenericData.get().resolveUnion(s, v)) : s;
    }

    private static void write(JsonGenerator g, Object v, Schema s) throws IOException {
        if (v == null) {
            g.writeNull();
            return;
        }
        LogicalType logical = s.getLogicalType();
        switch (s.getType()) {
            case UNION -> write(g, v, unionBranch(v, s));
            case NULL -> g.writeNull();
            case RECORD -> {
                g.writeStartObject();
                IndexedRecord r = (IndexedRecord) v;
                for (Schema.Field f : s.getFields()) {
                    g.writeFieldName(f.name());
                    write(g, r.get(f.pos()), f.schema());
                }
                g.writeEndObject();
            }
            case MAP -> {
                g.writeStartObject();
                for (Map.Entry<?, ?> e : ((Map<?, ?>) v).entrySet()) {
                    g.writeFieldName(String.valueOf(e.getKey()));
                    write(g, e.getValue(), s.getValueType());
                }
                g.writeEndObject();
            }
            case ARRAY -> {
                g.writeStartArray();
                for (Object item : (Collection<?>) v) write(g, item, s.getElementType());
                g.writeEndArray();
            }
            case ENUM, STRING -> g.writeString(v.toString());
            case BYTES -> writeBytes(g, toArray((ByteBuffer) v), logical);
            case FIXED -> writeBytes(g, ((GenericFixed) v).bytes(), logical);
            case INT -> {
                String time = logicalText(logical, (Integer) v);
                if (time != null) g.writeString(time);
                else g.writeNumber((Integer) v);
            }
            case LONG -> {
                String time = logicalText(logical, (Long) v);
                if (time != null) g.writeString(time);
                else g.writeNumber((Long) v);
            }
            case FLOAT -> {
                float f = (Float) v;
                if (Float.isFinite(f)) g.writeNumber(f);
                else g.writeString(Float.toString(f));
            }
            case DOUBLE -> {
                double d = (Double) v;
                if (Double.isFinite(d)) g.writeNumber(d);
                else g.writeString(Double.toString(d));
            }
            case BOOLEAN -> g.writeBoolean((Boolean) v);
        }
    }

    /** Dates and times as ISO text, or null to show the raw number (no logical type, or a value out of range). */
    private static String logicalText(LogicalType logical, long v) {
        try {
            return switch (logical) {
                case LogicalTypes.Date t -> LocalDate.ofEpochDay(v).toString();
                case LogicalTypes.TimeMillis t -> LocalTime.ofNanoOfDay(Math.multiplyExact(v, 1_000_000L)).toString();
                case LogicalTypes.TimeMicros t -> LocalTime.ofNanoOfDay(Math.multiplyExact(v, 1000L)).toString();
                case LogicalTypes.TimestampMillis t -> Instant.ofEpochMilli(v).toString();
                case LogicalTypes.TimestampMicros t -> Instant.EPOCH.plus(v, ChronoUnit.MICROS).toString();
                case LogicalTypes.LocalTimestampMillis t ->
                        LocalDateTime.ofInstant(Instant.ofEpochMilli(v), ZoneOffset.UTC).toString();
                case LogicalTypes.LocalTimestampMicros t ->
                        LocalDateTime.ofInstant(Instant.EPOCH.plus(v, ChronoUnit.MICROS), ZoneOffset.UTC).toString();
                case null, default -> null;
            };
        } catch (DateTimeException | ArithmeticException e) {
            return null;
        }
    }

    private static void writeBytes(JsonGenerator g, byte[] b, LogicalType logical) throws IOException {
        if (logical instanceof LogicalTypes.Decimal d && b.length > 0) {
            g.writeNumber(new BigDecimal(new BigInteger(b), d.getScale()));
        } else {
            g.writeString(HexFormat.of().formatHex(b));
        }
    }

    private static byte[] toArray(ByteBuffer buffer) {
        ByteBuffer b = buffer.duplicate();
        byte[] out = new byte[b.remaining()];
        b.get(out);
        return out;
    }

    /** The first {@code maxBytes} of UTF-8 text, without splitting a character. */
    private static String cut(byte[] utf8, int maxBytes) {
        int end = Math.max(0, maxBytes);
        while (end > 0 && (utf8[end] & 0xC0) == 0x80) end--;
        return new String(Arrays.copyOf(utf8, end), StandardCharsets.UTF_8);
    }

    /**
     * Collects generated JSON and stops it once there is more than the display limit. A UTF-8 character takes at
     * least one byte, so past {@code maxBytes} characters the text is certainly over {@code maxBytes} bytes.
     */
    private static final class BudgetWriter extends Writer {
        static final class Full extends IOException {
            Full() {
                super(null, null);
            }
        }

        private final StringBuilder sb = new StringBuilder();
        private final int limit;
        private long produced;

        BudgetWriter(int maxBytes) {
            this.limit = Math.max(0, maxBytes) + 1;
        }

        @Override
        public void write(char[] buf, int off, int len) throws IOException {
            produced += len;
            int room = limit - sb.length();
            if (room > 0) sb.append(buf, off, Math.min(len, room));
            if (sb.length() >= limit) throw new Full();
        }

        String text() { return sb.toString(); }
        long produced() { return produced; }

        @Override public void flush() {}
        @Override public void close() {}
    }

    private static void setIfAbsent(String key, String value) {
        if (System.getProperty(key) == null) System.setProperty(key, value);
    }

    /** A schema that is not Avro. */
    private static final class NotAvro extends Exception {
        NotAvro(String message) {
            super(message, null, false, false);
        }
    }

    /** Why there is no schema for an id; the message is shown to the user. */
    private static final class LookupFailed extends Exception {
        LookupFailed(String message) {
            super(message, null, false, false);
        }
    }

    @Override
    public void close() {
        if (resource == null) return;
        try {
            resource.close();
        } catch (Exception e) {
            // closing on shutdown: nothing useful to do
        }
    }
}
