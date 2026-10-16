package eventstore

import fs2.Chunk

private def nonblank(
    value: String,
    name: String
): Either[StoreError.InvalidArgument, String] =
  Either.cond(
    Option(value).exists(_.trim.nonEmpty),
    value,
    StoreError.InvalidArgument(s"$name must not be blank")
  )

private def nonnegative(
    value: Long,
    name: String
): Either[StoreError.InvalidArgument, Long] =
  Either.cond(
    value >= 0,
    value,
    StoreError.InvalidArgument(s"$name must be nonnegative")
  )

opaque type StreamId = String

object StreamId:
  def from(value: String): Either[StoreError.InvalidArgument, StreamId] =
    nonblank(value, "Stream ID")

  /** For known-valid constants. Use from for untrusted input. */
  def unsafeFrom(value: String): StreamId = from(value).fold(throw _, identity)
  extension (id: StreamId) def value: String = id

opaque type EventId = String

object EventId:
  def from(value: String): Either[StoreError.InvalidArgument, EventId] =
    nonblank(value, "Event ID")

  def unsafeFrom(value: String): EventId = from(value).fold(throw _, identity)

  extension (id: EventId) def value: String = id

opaque type EventType = String

object EventType:
  def from(value: String): Either[StoreError.InvalidArgument, EventType] =
    nonblank(value, "Event type")

  def unsafeFrom(value: String): EventType = from(value).fold(throw _, identity)

  extension (kind: EventType) def value: String = kind

/** Zero-based index in the global event array, distinct from a stream revision.
  */
opaque type EventPosition = Long

object EventPosition:
  def from(value: Long): Either[StoreError.InvalidArgument, EventPosition] =
    nonnegative(value, "Event position")

  def unsafeFrom(value: Long): EventPosition =
    from(value).fold(throw _, identity)

  given Ordering[EventPosition] = Ordering.Long

  extension (position: EventPosition) def value: Long = position

/** Zero-based revision local to one stream. */
opaque type Revision = Long
object Revision:
  val Zero: Revision = 0L

  def from(value: Long): Either[StoreError.InvalidArgument, Revision] =
    nonnegative(value, "Revision")

  def unsafeFrom(value: Long): Revision = from(value).fold(throw _, identity)

  given Ordering[Revision] = Ordering.Long

  extension (revision: Revision) def value: Long = revision

final case class NewEvent(
    payload: Chunk[Byte],
    eventType: EventType,
    eventId: Option[EventId] = None
)

final case class StoredEvent(
    streamId: StreamId,
    eventId: EventId,
    revision: Revision,
    payload: Chunk[Byte],
    eventType: EventType,
    position: EventPosition
)

enum ExpectedRevision:
  case Any, NoStream
  case Exact(revision: Revision)

sealed abstract class StoreError(message: String)
    extends RuntimeException(message)

object StoreError:
  final case class InvalidArgument(reason: String) extends StoreError(reason)
  final case class DuplicateEvent(eventId: EventId)
      extends StoreError(s"Event ID ${eventId.value} is already used")
  final case class RevisionConflict(
      streamId: StreamId,
      expected: ExpectedRevision,
      actual: Option[Revision]
  ) extends StoreError(
        s"Stream ${streamId.value}: expected $expected, actual revision $actual"
      )
