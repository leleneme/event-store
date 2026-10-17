# Scala Event Store

A small event store built with Scala, Cats Effect, and FS2. It supports appending, reading, and listening for events over TCP. Data is in-memory only.

## Requirements

- Java 17 or newer
- Scala 3.10.0
- sbt 2.0.12

> Scala and sbt versions are pinned in the project.

## Run

```sh
sbt run
```

The default address is `127.0.0.1:5252`. To change it:

```sh
sbt 'run --host 0.0.0.0 --port 6000'
```

Use `sbt 'run --help'` for options and `sbt test` to run the tests.

To build and run a standalone JAR:

```sh
sbt assembly
java -jar target/sc-event-store.jar --host 127.0.0.1 --port 5252
```

## Append and read events

With the server running, use the Python client in another terminal:

```sh
python examples/tcp_client.py \
  '{"op":"append","streamId":"orders","events":[{"eventType":"Placed","payloadBase64":"aGVsbG8="}]}' \
  '{"op":"read","streamId":"orders"}'
```

The payload above is `hello` encoded as Base64. Events get an ID automatically unless you supply `eventId`. IDs must be unique across the store.

To send 10,000 events and verify that they can be read back:

```sh
python examples/roundtrip.py
```

## Listen for events

Listen to a stream:

```sh
python examples/tcp_client.py --timeout 0 \
  '{"op":"listen","streamId":"orders"}'
```

Or listen to an event type across all streams:

```sh
python examples/tcp_client.py --timeout 0 \
  '{"op":"listenByType","eventType":"Placed"}'
```

Both replay existing events, then wait for new ones. `--timeout 0` lets the client wait indefinitely. Use another connection to append events. Stream listeners accept `fromRevision` to skip earlier events. Revisions start at zero and the starting revision is included.

## API

`EventStore[F]` is independent of TCP. `InMemoryEventStore` implements it.

- `append`: add a nonempty batch of events atomically.
- `fetch`: find an event by ID, optionally checking its stream.
- `read`: read a stream, optionally within an inclusive revision range.
- `readByType`: read events of a type in global append order.
- `listen`: read a stream and follow new events.
- `listenByType`: read events of a type and follow new ones.

Appends accept an `expectedRevision`: `"noStream"` requires a new stream, an integer must match its latest revision, and `"any"` skips the revision check. The default is `"any"`.

## TCP protocol

Messages use a four-byte big-endian length followed by UTF-8 JSON. Requests are limited to 1 MiB. Payload bytes use Base64 in `payloadBase64`.

Other requests supported by the Python client:

```json
{"op":"fetch","eventId":"a"}
{"op":"fetch","streamId":"orders","eventId":"a"}
{"op":"read","streamId":"orders","fromRevision":0,"toRevision":10}
{"op":"readByType","eventType":"Placed"}
```

Append, fetch, and read requests return `event` frames followed by `done`, or an `error` frame if the request fails. Read bounds default to the whole stream.

Subscriptions start with a `subscribed` frame, then send events without a final `done`. Each subscription needs its own connection. Keep the client's sending side open; closing it cancels the subscription.

This is a learning project. It has no persistence, authentication, or TLS.
