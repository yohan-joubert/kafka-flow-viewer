package io.kafkaflow.web;

import java.lang.reflect.RecordComponent;
import java.util.Collection;
import java.util.Map;

/** Minimal JSON writer for records, collections, maps, strings, numbers and booleans (keeps the jar dependency-free). */
public final class Json {

    private Json() {}

    public static String write(Object value) {
        StringBuilder sb = new StringBuilder(4096);
        write(sb, value);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Object v) {
        switch (v) {
            case null -> sb.append("null");
            case String s -> string(sb, s);
            case Boolean b -> sb.append(b);
            case Double d -> number(sb, d);
            case Float f -> number(sb, f.doubleValue());
            case Number n -> sb.append(n.longValue());
            case Enum<?> e -> string(sb, e.name());
            case Map<?, ?> m -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    string(sb, String.valueOf(e.getKey()));
                    sb.append(':');
                    write(sb, e.getValue());
                }
                sb.append('}');
            }
            case Collection<?> c -> {
                sb.append('[');
                boolean first = true;
                for (Object o : c) {
                    if (!first) sb.append(',');
                    first = false;
                    write(sb, o);
                }
                sb.append(']');
            }
            case Record r -> {
                sb.append('{');
                boolean first = true;
                for (RecordComponent rc : r.getClass().getRecordComponents()) {
                    if (!first) sb.append(',');
                    first = false;
                    string(sb, rc.getName());
                    sb.append(':');
                    try {
                        rc.getAccessor().setAccessible(true);
                        write(sb, rc.getAccessor().invoke(r));
                    } catch (ReflectiveOperationException ex) {
                        throw new IllegalStateException("Cannot serialize " + rc, ex);
                    }
                }
                sb.append('}');
            }
            default -> string(sb, v.toString());
        }
    }

    private static void number(StringBuilder sb, double d) {
        if (Double.isNaN(d) || Double.isInfinite(d)) {
            sb.append("null");
        } else if (d == Math.rint(d) && Math.abs(d) < 1e15) {
            sb.append((long) d);
        } else {
            sb.append(Math.round(d * 1000.0) / 1000.0); // 3 decimals is plenty for msg/s
        }
    }

    private static void string(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20 || c == ' ' || c == ' ') sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }
}
