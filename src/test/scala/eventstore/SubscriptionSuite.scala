package eventstore

import cats.data.NonEmptyList
import cats.effect.{Deferred, IO}
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite
import scala.concurrent.duration.*

class SubscriptionSuite extends CatsEffectSuite:
  override val munitIOTimeout = 10.seconds
  private val stream = StreamId.unsafeFrom("orders")
  private val other = StreamId.unsafeFrom("other")
  private val kind = EventType.unsafeFrom("Placed")
  private def batch(ids: String*): NonEmptyList[NewEvent] =
    NonEmptyList.fromListUnsafe(
      ids.toList.map(id =>
        NewEvent(Chunk.empty, kind, Some(EventId.unsafeFrom(id)))
      )
    )

  test(
    "listeners replay existing events in order and filter by stream or type"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      a <- store.append(stream, batch("a"))
      b <- store.append(other, batch("b"))
      _ <- store.append(
        other,
        NonEmptyList.one(NewEvent(Chunk.empty, EventType.unsafeFrom("Other")))
      )
      c <- store.append(stream, batch("c"))
      byStream <- store.listen(stream).take(2).compile.toList
      byType <- store.listenByType(kind).take(3).compile.toList
    yield
      assertEquals(byStream, a.toList ++ c.toList)
      assertEquals(byType, a.toList ++ b.toList ++ c.toList)
  }

  test("listeners wait for missing streams and future revisions") {
    for
      store <- InMemoryEventStore.create[IO]
      listener <- store
        .listen(stream, Revision.unsafeFrom(3))
        .take(2)
        .compile
        .toList
        .start
      _ <- store.append(other, batch("unrelated"))
      _ <- store.append(stream, batch("a", "b", "c"))
      expected <- store.append(stream, batch("d", "e"))
      actual <- listener.joinWithNever
    yield assertEquals(actual, expected.toList)
  }

  test(
    "slow replay catches every concurrent append without gaps or duplicates"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      seed <- store.append(stream, batch("seed"))
      replaying <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      listener <- store
        .listen(stream)
        .evalTap { event =>
          if event.revision.value == 0 then
            replaying.complete(()) *> release.get
          else IO.unit
        }
        .take(201)
        .compile
        .toList
        .start
      _ <- replaying.get
      // All these updates happen while the subscriber cannot pull notifications.
      appended <- (0 until 100).toList.parTraverse(i =>
        store.append(stream, batch(s"$i-a", s"$i-b"))
      )
      _ <- release.complete(())
      actual <- listener.joinWithNever
    yield
      val expected =
        (seed.toList ++ appended.flatMap(_.toList)).sortBy(_.position)
      assertEquals(actual, expected)
      assertEquals(actual.map(_.revision.value), (0L until 201L).toList)
  }

  test(
    "slow type replay catches up across streams while ignoring other types"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      seed <- store.append(stream, batch("seed"))
      replaying <- Deferred[IO, Unit]
      release <- Deferred[IO, Unit]
      listener <- store
        .listenByType(kind)
        .evalTap { event =>
          if event.eventId == seed.head.eventId then
            replaying.complete(()) *> release.get
          else IO.unit
        }
        .take(51)
        .compile
        .toList
        .start
      _ <- replaying.get
      appended <- (0 until 50).toList.parTraverse { i =>
        store.append(
          StreamId.unsafeFrom(s"stream-$i"),
          NonEmptyList.of(
            NewEvent(Chunk.empty, EventType.unsafeFrom("Other")),
            batch(s"e-$i").head
          )
        )
      }
      _ <- release.complete(())
      actual <- listener.joinWithNever
    yield
      val expected =
        (seed.toList ++ appended.flatMap(_.toList).filter(_.eventType == kind))
          .sortBy(_.position)
      assertEquals(actual, expected)
  }

  test(
    "multiple listeners receive the same events and cancellation does not affect writers"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      first <- store.listen(stream).take(3).compile.toList.start
      second <- store.listenByType(kind).take(3).compile.toList.start
      cancelled <- store.listen(other).compile.drain.start
      _ <- cancelled.cancel.timeout(1.second)
      a <- store.append(stream, batch("a"))
      rejected <- store.append(stream, batch("a")).attempt
      bc <- store.append(stream, batch("b", "c"))
      one <- first.joinWithNever
      two <- second.joinWithNever
    yield
      assert(rejected.isLeft)
      assertEquals(one, a.toList ++ bc.toList)
      assertEquals(two, one)
  }
