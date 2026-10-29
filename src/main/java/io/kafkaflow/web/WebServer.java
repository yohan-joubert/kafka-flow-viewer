package io.kafkaflow.web;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Serves the UI and pushes each new snapshot to browsers over Server-Sent Events.
 * Uses the JDK's built-in HTTP server with one virtual thread per connection.
 *
 * <ul>
 *   <li>{@code GET /}            the UI (static files from /web on the classpath)</li>
 *   <li>{@code GET /api/snapshot} latest snapshot as JSON</li>
 *   <li>{@code GET /api/stream}   text/event-stream, one "snapshot" event per poll</li>
 * </ul>
 */
public final class WebServer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(WebServer.class);
    private static final Map<String, String> TYPES = Map.of(
            "html", "text/html; charset=utf-8",
            "js", "text/javascript; charset=utf-8",
            "css", "text/css; charset=utf-8",
            "svg", "image/svg+xml",
            "ico", "image/x-icon");

    private final HttpServer server;
    private final Supplier<String> latestJson;
    private final Set<BlockingQueue<String>> subscribers = ConcurrentHashMap.newKeySet();

    public WebServer(String host, int port, Supplier<String> latestJson) throws IOException {
        this.latestJson = latestJson;
        this.server = HttpServer.create(new InetSocketAddress(host, port), 64);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/api/snapshot", this::snapshot);
        server.createContext("/api/stream", this::stream);
        server.createContext("/", this::staticFile);
    }

    public void start() {
        server.start();
    }

    public int port() {
        return server.getAddress().getPort();
    }

    /** Sends a snapshot to every connected browser. Slow clients only ever keep the latest one. */
    public void publish(String json) {
        for (BlockingQueue<String> q : subscribers) {
            q.clear();
            q.offer(json);
        }
    }

    public int clientCount() {
        return subscribers.size();
    }

    private void snapshot(HttpExchange ex) throws IOException {
        String json = latestJson.get();
        byte[] body = (json == null ? "{}" : json).getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.sendResponseHeaders(200, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    private void stream(HttpExchange ex) throws IOException {
        ex.getResponseHeaders().set("Content-Type", "text/event-stream; charset=utf-8");
        ex.getResponseHeaders().set("Cache-Control", "no-store");
        ex.getResponseHeaders().set("X-Accel-Buffering", "no"); // disable buffering behind nginx
        ex.sendResponseHeaders(200, 0);

        BlockingQueue<String> queue = new LinkedBlockingQueue<>(1);
        String current = latestJson.get();
        if (current != null) queue.offer(current);
        subscribers.add(queue);
        try (OutputStream os = ex.getResponseBody()) {
            os.write("retry: 3000\n\n".getBytes(StandardCharsets.UTF_8));
            os.flush();
            while (true) {
                String json = queue.poll(15, TimeUnit.SECONDS);
                String frame = json == null ? ": keep-alive\n\n" : "event: snapshot\ndata: " + json + "\n\n";
                os.write(frame.getBytes(StandardCharsets.UTF_8));
                os.flush();
            }
        } catch (IOException e) {
            // browser went away
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            subscribers.remove(queue);
            ex.close();
        }
    }

    private void staticFile(HttpExchange ex) throws IOException {
        String path = ex.getRequestURI().getPath();
        if (path.equals("/")) path = "/index.html";
        if (path.contains("..")) {
            send(ex, 400, "Bad path");
            return;
        }
        try (InputStream in = WebServer.class.getResourceAsStream("/web" + path)) {
            if (in == null) {
                send(ex, 404, "Not found");
                return;
            }
            byte[] body = in.readAllBytes();
            String ext = path.substring(path.lastIndexOf('.') + 1);
            ex.getResponseHeaders().set("Content-Type", TYPES.getOrDefault(ext, "application/octet-stream"));
            ex.getResponseHeaders().set("Cache-Control", "no-cache");
            ex.sendResponseHeaders(200, body.length);
            try (OutputStream os = ex.getResponseBody()) {
                os.write(body);
            }
        }
    }

    private static void send(HttpExchange ex, int status, String text) throws IOException {
        byte[] body = text.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(status, body.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(body);
        }
    }

    @Override
    public void close() {
        log.info("Stopping web server");
        server.stop(0);
    }
}
