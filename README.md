# Scala Event Store

A small and simple in-memory event store experiment using Scala 3, Cats Effect, and FS2, with a TCP interface

`EventStore[F]` is a transport-independent API. `InMemoryEventStore` implements it using a Cats Effect `Ref`. The TCP adapter decodes requests, calls this API, and streams responses. The in-memory implementation uses FS2 and Circe.

## Run

```sh
sbt test
sbt 'runMain Main'
sbt 'runMain eventstore.tcp.ServerMain'
```

## API

```scala
import cats.data.NonEmptyList
import cats.effect.IO
import eventstore.*
import fs2.Chunk

val example: IO[List[StoredEvent]] =
  for
    store <- InMemoryEventStore.create[IO]
    stream <- IO.fromEither(StreamId.from("order-123")) // aggregate ID
    eventType <- IO.fromEither(EventType.from("OrderPlaced"))
    lastRevision <- IO.fromEither(Revision.from(10))
    appended <- store.append(
      stream,
      NonEmptyList.one(NewEvent(Chunk[Byte](1, 2, 3), eventType = eventType)),
      ExpectedRevision.NoStream
    )
    event <- store.fetch(stream, appended.head.eventId)
    events <- store.read(stream, fromRevision = Revision.Zero, toRevision = Some(lastRevision))
      .compile.toList
  yield events
```

- `append(streamId, events, expectedRevision)` appends one or more events as an atomic batch, returning the stored events with their IDs and revisions.
- `fetch(eventId)` looks up one event globally, returning `None` if absent.
- `fetch(streamId, eventId)` verifies that the event belongs to the  requested stream; a different stream returns `None`.
- `readByType(eventType)` returns matching events in any stream in append order, using a event type index.
- `read(streamId, fromRevision, toRevision)` returns a FS2 stream in append order. Bounds are inclusive; the default reads the entire stream. Missing streams and ranges past the last event return an empty stream.

## TCP interface

Run `sbt 'runMain eventstore.tcp.ServerMain'` to listen on `127.0.0.1:5252`.

Each message is a four-byte big-endian length followed by UTF-8 encoded JSON payload. Request bodies must be between 1 byte and 1 MiB. Payload bytes use standard Base64 in `payloadBase64` since events can store any byte blob; IDs and types are JSON strings.

Requests:

```json
{"op":"append","streamId":"orders","expectedRevision":"noStream","events":[{"eventId":"a","eventType":"Placed","payloadBase64":"aGVsbG8="}]}
{"op":"fetch","eventId":"a"}
{"op":"fetch","streamId":"orders","eventId":"a"}
{"op":"read","streamId":"orders","fromRevision":0,"toRevision":10}
{"op":"readByType","eventType":"Placed"}
```

`expectedRevision` defaults to `"any"`, accepts `"noStream"`, or a nonnegative integer for an exact revision. `eventId` is optional on appended events; `fromRevision` defaults to zero and `toRevision` defaults to unbounded.

Successful appends, fetch, and read operations all return zero or more event frames, then one terminal `done` frame. Append returns the events it committed. An absent fetch or empty query returns only `done`.

```json
{"type":"event","event":{"streamId":"orders","eventId":"a","eventType":"Placed","revision":0,"position":0,"payloadBase64":"aGVsbG8="}}
{"type":"done"}
```

Failures return a error frame instead of `done`:

```json
{"type":"error","code":"duplicate_event","message":"Event ID a is already used"}
```

Error codes include `invalid_request`, `invalid_argument`, `duplicate_event`,`revision_conflict`, and `invalid_frame`. Invalid JSON, unknown operations; Domain errors leave the connection available for the next request. 

Connections are processed concurrently. Requests on each connection run in order and responses never interleave. Clients should consume a terminal frame before sending the next request.

The Python example handles framing and prints each response as it arrives:

```sh
python3 examples/tcp_client.py \
  '{"op":"append","streamId":"orders","events":[{"eventId":"a","eventType":"Placed","payloadBase64":"aGVsbG8="}]}' \
  '{"op":"read","streamId":"orders"}'
```

The example uses a 10-second socket timeout. The server has no idle timeout,
authentication, or TLS and defaults to loopback for local experiments.

To append 10,000 sample events in batches of 500, then reconnect and read them all back with ID, type, payload, stream, and revision verification:

```sh
python3 examples/roundtrip.py
```
