import cats.data.NonEmptyList
import cats.effect.{IO, IOApp}
import eventstore.*
import fs2.Chunk

object Main extends IOApp.Simple:
  def run: IO[Unit] =
    val streamId = StreamId.unsafeFrom("order-123")
    for
      store <- InMemoryEventStore.create[IO]
      appended <- store.append(
        streamId,
        NonEmptyList.of(
          NewEvent(
            Chunk.from("OrderPlaced".getBytes("UTF-8")),
            EventType.unsafeFrom("OrderPlaced")
          ),
          NewEvent(
            Chunk.from("OrderPaid".getBytes("UTF-8")),
            EventType.unsafeFrom("OrderPaid")
          )
        ),
        ExpectedRevision.NoStream
      )
      _ <- IO.println(
        s"Appended ${appended.length} events to ${streamId.value}"
      )
      _ <- store.read(streamId).evalMap(IO.println).compile.drain
      _ <- store
        .readByType(EventType.unsafeFrom("OrderPaid"))
        .evalMap(IO.println)
        .compile
        .drain
      found <- store.fetch(streamId, appended.head.eventId)
      _ <- IO.println(s"Fetched: $found")
    yield ()
