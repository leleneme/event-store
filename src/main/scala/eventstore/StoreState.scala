package eventstore

import cats.data.NonEmptyList
import fs2.{Chunk, Pull, Pure, Stream}

/** Immutable event array and its indices, committed together in a single Ref.
  */
private[eventstore] final case class StoreState(
    events: Vector[StoredEvent] = Vector.empty,
    streams: Map[StreamId, Vector[EventPosition]] = Map.empty,
    eventIds: Map[EventId, EventPosition] = Map.empty,
    eventTypes: Map[EventType, Vector[EventPosition]] = Map.empty
):
  // Vector is Int-indexed; only positions created by append reach this method.
  private def at(position: EventPosition): StoredEvent = events(
    position.value.toInt
  )

  def fetch(eventId: EventId): Option[StoredEvent] =
    eventIds.get(eventId).map(at)

  private def resolve(
      positions: Vector[EventPosition],
      start: Int,
      end: Int
  ): Stream[Pure, StoredEvent] =
    // Bound both lookup work and temporary output to a chunk, not the result size.
    def go(offset: Int): Pull[Pure, StoredEvent, Unit] =
      if offset >= end then Pull.done
      else
        val next = (offset.toLong + 64).min(end.toLong).toInt
        val chunk = Chunk.from(
          Vector.tabulate(next - offset)(i => at(positions(offset + i)))
        )
        Pull.output(chunk) >> Pull.suspend(go(next))
    Pull.suspend(go(start)).stream

  def read(
      streamId: StreamId,
      from: Revision,
      to: Option[Revision]
  ): Stream[Pure, StoredEvent] =
    val positions = streams.getOrElse(streamId, Vector.empty)
    val start = from.value.min(positions.size.toLong).toInt
    val end = to.fold(positions.size)(n =>
      n.value.min(positions.size.toLong - 1).toInt + 1
    )
    resolve(positions, start, end)

  def readByType(eventType: EventType): Stream[Pure, StoredEvent] =
    val positions = eventTypes.getOrElse(eventType, Vector.empty)
    resolve(positions, 0, positions.size)

  def append(
      streamId: StreamId,
      batch: NonEmptyList[(EventId, NewEvent)],
      expected: ExpectedRevision
  ): Either[StoreError, (StoreState, NonEmptyList[StoredEvent])] =
    val positions = streams.getOrElse(streamId, Vector.empty)
    val actual = Option.when(positions.nonEmpty)(
      Revision.unsafeFrom(positions.size.toLong - 1)
    )
    val matches = expected match
      case ExpectedRevision.Any      => true
      case ExpectedRevision.NoStream => actual.isEmpty
      case ExpectedRevision.Exact(n) => actual.contains(n)
    val (_, duplicate) =
      batch.toList.foldLeft((Set.empty[EventId], Option.empty[EventId])) {
        case ((seen, found), (id, _)) =>
          (
            seen + id,
            found.orElse(Option.when(seen(id) || eventIds.contains(id))(id))
          )
      }
    if !matches then
      Left(StoreError.RevisionConflict(streamId, expected, actual))
    else if events.size.toLong + batch.length.toLong > Int.MaxValue then
      Left(
        StoreError.InvalidArgument("In-memory event array capacity exceeded")
      )
    else
      duplicate match
        case Some(id) => Left(StoreError.DuplicateEvent(id))
        case None     =>
          val appended = batch.zipWithIndex.map { case ((id, event), index) =>
            StoredEvent(
              streamId,
              id,
              Revision.unsafeFrom(positions.size.toLong + index),
              event.payload,
              event.eventType,
              EventPosition.unsafeFrom(events.size.toLong + index)
            )
          }
          val batchEvents = appended.toList.toVector
          val nextTypes = batchEvents.foldLeft(eventTypes) { (index, event) =>
            index.updated(
              event.eventType,
              index.getOrElse(event.eventType, Vector.empty) :+ event.position
            )
          }
          val next = copy(
            events = events ++ batchEvents,
            streams = streams
              .updated(streamId, positions ++ batchEvents.map(_.position)),
            eventIds = eventIds ++ batchEvents.map(event =>
              event.eventId -> event.position
            ),
            eventTypes = nextTypes
          )
          Right((next, appended))
