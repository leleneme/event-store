package eventstore

import cats.data.NonEmptyList
import cats.effect.IO
import cats.syntax.all.*
import fs2.Chunk
import munit.CatsEffectSuite

class EventStoreSuite extends CatsEffectSuite:
  private def rev(value: Long): Revision = Revision.unsafeFrom(value)
  private def pos(value: Long): EventPosition = EventPosition.unsafeFrom(value)
  private def kind(value: String): EventType = EventType.unsafeFrom(value)
  private val stream = StreamId.unsafeFrom("order-1")
  private def event(id: String): NewEvent =
    NewEvent(
      Chunk[Byte](1, 2, 3),
      kind("OrderChanged"),
      Some(EventId.unsafeFrom(id))
    )
  private def batch(ids: String*): NonEmptyList[NewEvent] =
    NonEmptyList.fromListUnsafe(ids.toList.map(event))

  test(
    "append preserves payloads and IDs, assigns contiguous revisions, and supports lookup"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      first <- store.append(stream, batch("a", "b"))
      second <- store.append(stream, batch("c"))
      all <- store.read(stream).compile.toList
      found <- store.fetch(stream, EventId.unsafeFrom("b"))
    yield
      assertEquals(all, first.toList ++ second.toList)
      assertEquals(all.map(_.revision.value), List(0L, 1L, 2L))
      assertEquals(all.map(_.payload), List.fill(3)(Chunk[Byte](1, 2, 3)))
      assertEquals(found, Some(all(1)))
  }

  test("omitted IDs are generated and can be used to fetch events") {
    for
      store <- InMemoryEventStore.create[IO]
      saved <- store.append(
        stream,
        NonEmptyList.of(
          NewEvent(Chunk.empty, kind("OrderChanged")),
          NewEvent(Chunk.empty, kind("OrderChanged"))
        )
      )
      found <- store.fetch(stream, saved.head.eventId)
    yield
      assert(saved.toList.forall(_.eventId.value.nonEmpty))
      assertEquals(saved.toList.map(_.eventId).distinct.size, 2)
      assertEquals(found, Some(saved.head))
  }

  test("stream reads honor inclusive bounds and return empty past the end") {
    for
      store <- InMemoryEventStore.create[IO]
      _ <- store.append(stream, batch("a", "b", "c", "d"))
      range <- store.read(stream, rev(1), Some(rev(2))).compile.toList
      tail <- store.read(stream, rev(2)).compile.toList
      empty <- store.read(stream, rev(10)).compile.toList
    yield
      assertEquals(range.map(_.revision.value), List(1L, 2L))
      assertEquals(tail.map(_.revision.value), List(2L, 3L))
      assertEquals(empty, Nil)
  }

  test(
    "missing events and streams are empty; scoped fetch verifies stream ownership"
  ) {
    val other = StreamId.unsafeFrom("order-2")
    for
      store <- InMemoryEventStore.create[IO]
      missing <- store.fetch(stream, EventId.unsafeFrom("a"))
      empty <- store.read(stream).compile.toList
      _ <- store.append(stream, batch("a"))
      saved <- store.append(other, batch("b"))
      found <- store.fetch(other, EventId.unsafeFrom("b"))
      wrongStream <- store.fetch(other, EventId.unsafeFrom("a"))
    yield
      assertEquals(missing, None)
      assertEquals(empty, Nil)
      assertEquals(found, Some(saved.head))
      assertEquals(wrongStream, None)
  }

  test("duplicate IDs within a batch reject the entire append") {
    for
      store <- InMemoryEventStore.create[IO]
      result <- store.append(stream, batch("a", "a")).attempt
      all <- store.read(stream).compile.toList
    yield
      assertEquals(
        result,
        Left(StoreError.DuplicateEvent(EventId.unsafeFrom("a")))
      )
      assertEquals(all, Nil)
  }

  test("an existing ID rejects the whole batch without consuming revisions") {
    for
      store <- InMemoryEventStore.create[IO]
      _ <- store.append(stream, batch("a"))
      result <- store.append(stream, batch("b", "a")).attempt
      next <- store.append(stream, batch("c"))
      all <- store.read(stream).compile.toList
    yield
      assertEquals(
        result,
        Left(StoreError.DuplicateEvent(EventId.unsafeFrom("a")))
      )
      assertEquals(next.head.revision.value, 1L)
      assertEquals(all.map(_.eventId.value), List("a", "c"))
  }

  test(
    "expected revisions distinguish creation, matching revisions, and conflicts"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      missing <- store
        .append(stream, batch("x"), ExpectedRevision.Exact(rev(0)))
        .attempt
      _ <- store.append(stream, batch("a"), ExpectedRevision.NoStream)
      exists <- store
        .append(stream, batch("x"), ExpectedRevision.NoStream)
        .attempt
      _ <- store.append(stream, batch("b"), ExpectedRevision.Exact(rev(0)))
      stale <- store
        .append(stream, batch("x"), ExpectedRevision.Exact(rev(0)))
        .attempt
      all <- store.read(stream).compile.toList
    yield
      assertEquals(
        missing,
        Left(
          StoreError.RevisionConflict(
            stream,
            ExpectedRevision.Exact(rev(0)),
            None
          )
        )
      )
      assertEquals(
        exists,
        Left(
          StoreError.RevisionConflict(
            stream,
            ExpectedRevision.NoStream,
            Some(rev(0))
          )
        )
      )
      assertEquals(
        stale,
        Left(
          StoreError.RevisionConflict(
            stream,
            ExpectedRevision.Exact(rev(0)),
            Some(rev(1))
          )
        )
      )
      assertEquals(all.map(_.eventId.value), List("a", "b"))
  }

  test("concurrent batches remain contiguous and no events are lost") {
    for
      store <- InMemoryEventStore.create[IO]
      batches <- (0 until 100).toList.parTraverse { n =>
        store.append(stream, batch(s"$n-a", s"$n-b"))
      }
      all <- store.read(stream).compile.toList
    yield
      assertEquals(all.map(_.revision.value), (0L until 200L).toList)
      assertEquals(all.map(_.eventId).distinct.size, 200)
      assert(
        batches.forall(b => b.last.revision.value == b.head.revision.value + 1)
      )
  }

  test(
    "concurrent writers expecting the same revision have exactly one winner"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      _ <- store.append(stream, batch("initial"))
      attempts <- (0 until 50).toList.parTraverse { n =>
        store
          .append(stream, batch(s"$n"), ExpectedRevision.Exact(rev(0)))
          .attempt
      }
      all <- store.read(stream).compile.toList
    yield
      assertEquals(attempts.count(_.isRight), 1)
      assert(
        attempts
          .collect { case Left(error) => error }
          .forall(_.isInstanceOf[StoreError.RevisionConflict])
      )
      assertEquals(all.size, 2)
  }

  test(
    "reads are lazy finite snapshots that do not include appends made during consumption"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      read = store.read(stream)
      _ <- store.append(stream, batch("a", "b"))
      seen <- read
        .evalTap { e =>
          if e.revision.value == 0 then store.append(stream, batch("c")).void
          else IO.unit
        }
        .compile
        .toList
      after <- store.read(stream).compile.toList
    yield
      assertEquals(seen.map(_.eventId.value), List("a", "b"))
      assertEquals(after.map(_.eventId.value), List("a", "b", "c"))
  }

  test("reversed revision ranges fail with domain errors") {
    for
      store <- InMemoryEventStore.create[IO]
      result <- store.read(stream, rev(2), Some(rev(1))).compile.drain.attempt
    yield assert(
      result.swap.toOption.exists(_.isInstanceOf[StoreError.InvalidArgument])
    )
  }

  test(
    "indices fetch interleaved streams and event types in the correct order"
  ) {
    val other = StreamId.unsafeFrom("order-2")
    for
      store <- InMemoryEventStore.create[IO]
      a <- store.append(
        stream,
        NonEmptyList.one(event("a").copy(eventType = kind("Placed")))
      )
      b <- store.append(
        other,
        NonEmptyList.one(event("b").copy(eventType = kind("Paid")))
      )
      cd <- store.append(
        stream,
        NonEmptyList.of(
          event("c").copy(eventType = kind("Paid")),
          event("d").copy(eventType = kind("Placed"))
        )
      )
      byId <- store.fetch(EventId.unsafeFrom("b"))
      firstStream <- store.read(stream, rev(1), Some(rev(2))).compile.toList
      paid <- store.readByType(kind("Paid")).compile.toList
      placed <- store.readByType(kind("Placed")).compile.toList
      missing <- store.readByType(kind("Unknown")).compile.toList
    yield
      assertEquals(
        (a.toList ++ b.toList ++ cd.toList).map(_.position.value),
        List(0L, 1L, 2L, 3L)
      )
      assertEquals(cd.toList.map(_.revision.value), List(1L, 2L))
      assertEquals(byId, Some(b.head))
      assertEquals(firstStream, cd.toList)
      assertEquals(paid.map(_.eventId.value), List("b", "c"))
      assertEquals(placed.map(_.eventId.value), List("a", "d"))
      assertEquals(missing, Nil)
  }

  test(
    "global duplicate rejection leaves every index and the next position unchanged"
  ) {
    val other = StreamId.unsafeFrom("order-2")
    for
      store <- InMemoryEventStore.create[IO]
      _ <- store.append(stream, batch("a"))
      rejected <- store
        .append(
          other,
          NonEmptyList.of(
            event("b").copy(eventType = kind("Rejected")),
            event("a")
          )
        )
        .attempt
      absent <- store.fetch(EventId.unsafeFrom("b"))
      rejectedType <- store.readByType(kind("Rejected")).compile.toList
      empty <- store.read(other).compile.toList
      next <- store.append(other, batch("b"), ExpectedRevision.NoStream)
    yield
      assertEquals(
        rejected,
        Left(StoreError.DuplicateEvent(EventId.unsafeFrom("a")))
      )
      assertEquals(absent, None)
      assertEquals(rejectedType, Nil)
      assertEquals(empty, Nil)
      assertEquals(next.head.position.value, 1L)
      assertEquals(next.head.revision.value, 0L)
  }

  test("Long revision bounds are clamped before converting to array indices") {
    for
      store <- InMemoryEventStore.create[IO]
      saved <- store.append(stream, batch("a", "b"))
      all <- store
        .read(stream, Revision.Zero, Some(rev(Long.MaxValue)))
        .compile
        .toList
      empty <- store
        .read(stream, rev(Long.MaxValue), Some(rev(Long.MaxValue)))
        .compile
        .toList
      missing <- store
        .read(
          StreamId.unsafeFrom("missing"),
          Revision.Zero,
          Some(rev(Long.MaxValue))
        )
        .compile
        .toList
    yield
      assertEquals(all, saved.toList)
      assertEquals(empty, Nil)
      assertEquals(missing, Nil)
  }

  test("type queries capture lazy snapshots across streams") {
    for
      store <- InMemoryEventStore.create[IO]
      read = store.readByType(kind("OrderChanged"))
      _ <- store.append(stream, batch("a", "b"))
      seen <- read
        .evalTap { e =>
          if e.position.value == 0 then
            store.append(StreamId.unsafeFrom("other"), batch("c")).void
          else IO.unit
        }
        .compile
        .toList
      after <- store.readByType(kind("OrderChanged")).compile.toList
    yield
      assertEquals(seen.map(_.eventId.value), List("a", "b"))
      assertEquals(after.map(_.eventId.value), List("a", "b", "c"))
  }

  test(
    "concurrent writes across streams preserve global positions and all query results"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      batches <- (0 until 100).toList.parTraverse { n =>
        store.append(
          StreamId.unsafeFrom(s"stream-${n % 5}"),
          batch(s"$n-a", s"$n-b")
        )
      }
      byType <- store.readByType(kind("OrderChanged")).compile.toList
      byStream <- (0 until 5).toList.traverse(n =>
        store.read(StreamId.unsafeFrom(s"stream-$n")).compile.toList
      )
      byId <- byType.traverse(e => store.fetch(e.eventId))
    yield
      assertEquals(byType.map(_.position.value), (0L until 200L).toList)
      assertEquals(byType, batches.flatMap(_.toList).sortBy(_.position.value))
      assert(byStream.forall(_.map(_.revision.value) == (0L until 40L).toList))
      assertEquals(byStream.flatten.sortBy(_.position.value), byType)
      assertEquals(byId, byType.map(Some(_)))
      assert(
        batches.forall(b => b.last.position.value == b.head.position.value + 1)
      )
  }

  test("concurrent reuse of a global ID across streams has one winner") {
    for
      store <- InMemoryEventStore.create[IO]
      attempts <- (0 until 30).toList.parTraverse { n =>
        store.append(StreamId.unsafeFrom(s"stream-$n"), batch("shared")).attempt
      }
      byType <- store.readByType(kind("OrderChanged")).compile.toList
      byId <- store.fetch(EventId.unsafeFrom("shared"))
    yield
      assertEquals(attempts.count(_.isRight), 1)
      assertEquals(
        attempts.collect { case Left(error) => error },
        List.fill(29)(StoreError.DuplicateEvent(EventId.unsafeFrom("shared")))
      )
      assertEquals(byType.size, 1)
      assertEquals(byType.head.position.value, 0L)
      assertEquals(byId, byType.headOption)
  }

  test(
    "state indices point to the event array by stream revision, ID, and type"
  ) {
    def prepared(id: String, kind: String) = EventId.unsafeFrom(id) -> event(id)
      .copy(eventType = EventType.unsafeFrom(kind))
    val result = for
      first <- StoreState().append(
        stream,
        NonEmptyList.of(prepared("a", "Placed"), prepared("b", "Paid")),
        ExpectedRevision.Any
      )
      second <- first._1.append(
        StreamId.unsafeFrom("other"),
        NonEmptyList.one(prepared("c", "Paid")),
        ExpectedRevision.Any
      )
      third <- second._1.append(
        stream,
        NonEmptyList.one(prepared("d", "Paid")),
        ExpectedRevision.Any
      )
    yield third._1
    val state = result.toOption.get
    assertEquals(
      state.streams,
      Map(
        stream -> Vector(pos(0), pos(1), pos(3)),
        StreamId.unsafeFrom("other") -> Vector(pos(2))
      )
    )
    assertEquals(
      state.eventIds,
      Map(
        EventId.unsafeFrom("a") -> pos(0),
        EventId.unsafeFrom("b") -> pos(1),
        EventId.unsafeFrom("c") -> pos(2),
        EventId.unsafeFrom("d") -> pos(3)
      )
    )
    assertEquals(
      state.eventTypes,
      Map(
        kind("Placed") -> Vector(pos(0)),
        kind("Paid") -> Vector(pos(1), pos(2), pos(3))
      )
    )
    assertEquals(state.events.map(_.position.value), Vector(0L, 1L, 2L, 3L))
  }

  test(
    "stream and type reads resolve only the requested chunks of the snapshot"
  ) {
    val saved = StoredEvent(
      stream,
      EventId.unsafeFrom("a"),
      rev(0),
      Chunk.empty,
      kind("T"),
      pos(0)
    )
    // An invalid suffix is a sentinel: eager resolution would fail before emitting the prefix.
    val positions = Vector.fill(64)(pos(0)) :+ pos(999)
    val snapshot = StoreState(
      events = Vector(saved),
      streams = Map(stream -> positions),
      eventTypes = Map(kind("T") -> positions)
    )
    List(
      snapshot.read(stream, Revision.Zero, None),
      snapshot.readByType(kind("T"))
    ).traverse_ { read =>
      for prefix <- read.covary[IO].take(64).compile.toList
      yield assertEquals(prefix, List.fill(64)(saved))
    }
  }

  test(
    "reevaluating a read captures a new snapshot, including across chunk boundaries"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      _ <- store.append(
        stream,
        NonEmptyList.fromListUnsafe(
          (0 until 130).toList.map(i => event(s"e-$i"))
        )
      )
      streamRead = store.read(stream)
      typeRead = store.readByType(kind("OrderChanged"))
      before <- streamRead
        .evalTap { e =>
          if e.revision.value == 0 then
            store.append(stream, batch("later")).void
          else IO.unit
        }
        .compile
        .toList
      after <- streamRead.compile.toList
      types <- typeRead.compile.toList
    yield
      assertEquals(before.map(_.revision.value), (0L until 130L).toList)
      assertEquals(after.map(_.revision.value), (0L until 131L).toList)
      assertEquals(types, after)
  }
