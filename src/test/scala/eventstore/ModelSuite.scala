package eventstore

import scala.compiletime.testing.typeChecks

class ModelSuite extends munit.FunSuite:
  test("string constructors reject blank and null values") {
    List("", " ", "\t\n", null).foreach { value =>
      assert(StreamId.from(value).isLeft)
      assert(EventId.from(value).isLeft)
      assert(EventType.from(value).isLeft)
    }
  }

  test("valid string values preserve whitespace and Unicode") {
    val value = " 注文-123 "
    assertEquals(StreamId.from(value).map(_.value), Right(value))
    assertEquals(EventId.from(value).map(_.value), Right(value))
    assertEquals(EventType.from(value).map(_.value), Right(value))
  }

  test(
    "numeric constructors reject negative values and accept their full nonnegative range"
  ) {
    List(-1L, Long.MinValue).foreach { value =>
      assert(Revision.from(value).isLeft)
      assert(EventPosition.from(value).isLeft)
    }
    List(0L, 1L, Long.MaxValue).foreach { value =>
      assertEquals(Revision.from(value).map(_.value), Right(value))
      assertEquals(EventPosition.from(value).map(_.value), Right(value))
    }
    assertEquals(Revision.Zero.value, 0L)
  }

  test("unsafe constructors still reject invalid values") {
    intercept[StoreError.InvalidArgument](StreamId.unsafeFrom(" "))
    intercept[StoreError.InvalidArgument](EventId.unsafeFrom(""))
    intercept[StoreError.InvalidArgument](EventType.unsafeFrom(""))
    intercept[StoreError.InvalidArgument](Revision.unsafeFrom(-1))
    intercept[StoreError.InvalidArgument](EventPosition.unsafeFrom(-1))
  }

  test(
    "opaque types prevent mixing revisions, positions, IDs, and raw values"
  ) {
    assert(
      !typeChecks(
        """import eventstore.*; val r: Revision = EventPosition.unsafeFrom(0)"""
      )
    )
    assert(
      !typeChecks(
        """import eventstore.*; val p: EventPosition = Revision.Zero"""
      )
    )
    assert(!typeChecks("""import eventstore.*; val r: Revision = 0L"""))
    assert(!typeChecks("""import eventstore.*; val id: StreamId = "orders""""))
    assert(
      !typeChecks(
        """import eventstore.*; val id: EventId = StreamId.unsafeFrom("orders")"""
      )
    )
    assert(
      !typeChecks(
        """import eventstore.*; val r = ExpectedRevision.Exact(EventPosition.unsafeFrom(0))"""
      )
    )
    assert(
      typeChecks(
        """import eventstore.*; val r = ExpectedRevision.Exact(Revision.Zero)"""
      )
    )
  }

  test("positions and revisions retain numeric ordering") {
    assertEquals(
      List(9L, 1L, 5L).map(Revision.unsafeFrom).sorted.map(_.value),
      List(1L, 5L, 9L)
    )
    assertEquals(
      List(9L, 1L, 5L).map(EventPosition.unsafeFrom).sorted.map(_.value),
      List(1L, 5L, 9L)
    )
  }
