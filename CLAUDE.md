# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

Java 21+, Maven. No linter is configured.

```bash
mvn package                                   # tests + target/kafka-flow-viewer.jar (shaded, runnable)
mvn test                                      # all tests
mvn test -Dtest=AvroDecoderTest               # one class
mvn test -Dtest='AvroDecoderTest#keepsNumbersExact'   # one method
java -jar target/kafka-flow-viewer.jar --demo           # simulated cluster, no Kafka needed (UI on :8080)
java -jar target/kafka-flow-viewer.jar -b localhost:9092 --registry http://localhost:8081
```

Dev cluster: `docker compose up -d` starts Kafka (`localhost:9092` for the host, `kafka:19092` between containers)
and a Confluent Schema Registry on `:8081`. Test traffic:
`java -cp target/kafka-flow-viewer.jar io.kafkaflow.tools.LoadGenerator localhost:9092 http://localhost:8081`.
It creates `demo.*` topics, a deliberately slow group, an exactly-once relay chain and, given the registry URL,
Avro on `demo.users-avro`.

The UI (`src/main/resources/web/`) is plain HTML/CSS/JS with no build step. It is served from the classpath, so
rebuild the jar (or run from `target/classes`) to see changes.

## Architecture

The data path is **poll → sample → calculate → publish**:

1. `KafkaFlowViewer` runs a single-threaded scheduler. Every `poll.interval.ms` it calls `ClusterSource.sample()`
   and gets a `ClusterSample` of raw counters: end offsets, committed offsets, group members and producer sequence
   numbers.
2. `FlowCalculator` is stateful and keeps the previous sample. It turns counter *deltas* into rates (EMA-smoothed),
   lag and links, producing a `FlowSnapshot`. Two conventions span the backend and `app.js`:
   - node ids are prefixed `t:` (topic), `g:` (group) and `p:` (producer);
   - `p:unidentified` is a synthetic producer for traffic that no identified producer explains.

   A group whose producer's `transactional.id` equals `<group>` or starts with `<group>-` is folded into that group
   as a processing step. This is how chains are drawn.
3. `WebServer` (JDK `HttpServer`, one virtual thread per request) pushes each snapshot over SSE (`/api/stream`).
   `/api/partitions` and `/api/sample` are registered by `KafkaFlowViewer` through `addQuery`.

`ClusterSource` has two implementations:
- `KafkaClusterSource`: AdminClient only, read-only, never joins a group.
- `SimulatedClusterSource`: `--demo`, also used by tests.

Sample messages use a separate group-less `KafkaConsumer` guarded by `sampleLock`. Decoding (and any Schema
Registry call) happens *after* the lock is released.

`SampledMessage.Payload` makes bytes readable. Its `encoding` is one of `null`, `text`, `avro` or `hex`; `problem`
says why bytes couldn't be decoded. `AvroDecoder` handles the Confluent wire format (byte 0 + 4-byte schema id):
- writer schemas come from `CachedSchemaRegistryClient`;
- failed lookups are cached (per id, or registry-wide for connectivity/auth errors);
- JSON is streamed with a byte budget;
- it never throws: failures fall back to hex plus a `problem`.

## Configuration

`AppConfig` merges three `java.util.Properties` layers, each overriding the previous: `default.properties` (which
documents every key), then a properties file (`--config`, or `./kafka-flow.properties`), then CLI flags parsed by
picocli (`AppConfig.Cli`) and `--set k=v`. Prefixed keys are
passed through with the prefix stripped:
- `kafka.*` go to the Kafka clients;
- `registry.*` go to the Schema Registry client, except `registry.url`. Their values are not trimmed, except
  `*.ms`/`*.sec`.

## Gotchas

- `web/Json` is a minimal hand-written writer: doubles are rounded to 3 decimals and other `Number`s go through
  `longValue()`. Use it only for the viewer's own API. Message contents are rendered with Jackson in
  `AvroDecoder`.
- Avro values are rendered server-side, and `app.js` must not `JSON.parse` them, because that would lose longs
  above 2^53. Everything inserted into the DOM goes through `esc()`. Schema names and problems come from the
  cluster.
- `kafka-clients` is declared directly so that 3.9.1 wins over the `-ccs` version the Confluent artifacts bring.
  `slf4j-api` 2.x is declared for the same reason: with kafka-clients' 1.7, `slf4j-simple` 2.x logs nothing.
- Confluent artifacts come from `https://packages.confluent.io/maven/` (declared in `pom.xml`).
- Keep the README tables (configuration keys, HTTP API, "How the numbers are measured") in sync with behaviour
  changes.
