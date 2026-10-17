package eventstore

import cats.data.NonEmptyList
import fs2.Stream

/** Transport-independent contract shared by local callers and future client
  * adapters.
  */
trait EventStore[F[_]]:
  /** Append a nonempty batch atomically. Event IDs must be globally unique. */
  def append(
      streamId: StreamId,
      events: NonEmptyList[NewEvent],
      expectedRevision: ExpectedRevision = ExpectedRevision.Any
  ): F[NonEmptyList[StoredEvent]]

  def fetch(streamId: StreamId, eventId: EventId): F[Option[StoredEvent]]

  def fetch(eventId: EventId): F[Option[StoredEvent]]

  /** Finite snapshot across all streams, ordered by global position. */
  def readByType(eventType: EventType): Stream[F, StoredEvent]

  /** Finite snapshot captured when evaluated. Both bounds are inclusive. */
  def read(
      streamId: StreamId,
      fromRevision: Revision = Revision.Zero,
      toRevision: Option[Revision] = None
  ): Stream[F, StoredEvent]

  /** Replay from the given revision, then follow future events until cancelled.
    */
  def listen(
      streamId: StreamId,
      fromRevision: Revision = Revision.Zero
  ): Stream[F, StoredEvent]

  /** Replay all matching events in global order, then follow future appends. */
  def listenByType(eventType: EventType): Stream[F, StoredEvent]
