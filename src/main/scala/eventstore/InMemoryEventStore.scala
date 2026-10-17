package eventstore

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.Async
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.SignallingRef

object InMemoryEventStore:
  // Allocation needs synchronous randomness and concurrent change notifications.
  def create[F[_]: Async]: F[EventStore[F]] =
    SecureRandom.of[F].flatMap { random =>
      given SecureRandom[F] = random
      SignallingRef[F, StoreState](StoreState()).map(new Impl[F](_))
    }

  private final class Impl[F[_]: MonadThrow: UUIDGen](
      state: SignallingRef[F, StoreState]
  ) extends EventStore[F]:
    def append(
        streamId: StreamId,
        events: NonEmptyList[NewEvent],
        expectedRevision: ExpectedRevision
    ): F[NonEmptyList[StoredEvent]] =
      for
        // Generate IDs outside Ref.modify, whose pure callback may be retried.
        prepared <- events.traverse { event =>
          event.eventId
            .fold(
              UUIDGen
                .randomUUID[F]
                .map(uuid => EventId.unsafeFrom(uuid.toString))
            )(_.pure[F])
            .map(id => (id, event))
        }
        result <- state.modify { current =>
          current.append(streamId, prepared, expectedRevision) match
            case Left(error)             => (current, Left(error))
            case Right((next, appended)) => (next, Right(appended))
        }
        appended <- MonadThrow[F].fromEither(result)
      yield appended

    def fetch(eventId: EventId): F[Option[StoredEvent]] =
      state.get.map(_.fetch(eventId))

    def fetch(streamId: StreamId, eventId: EventId): F[Option[StoredEvent]] =
      fetch(eventId).map(_.filter(_.streamId == streamId))

    def read(
        streamId: StreamId,
        fromRevision: Revision,
        toRevision: Option[Revision]
    ): Stream[F, StoredEvent] =
      val validRange = Either.cond(
        toRevision.forall(_.value >= fromRevision.value),
        (),
        StoreError.InvalidArgument(
          "End revision must be at least the start revision"
        )
      )
      Stream
        .eval(MonadThrow[F].fromEither(validRange) *> state.get)
        .flatMap(snapshot =>
          snapshot.read(streamId, fromRevision, toRevision).covary[F]
        )

    def readByType(eventType: EventType): Stream[F, StoredEvent] =
      Stream
        .eval(state.get)
        .flatMap(snapshot => snapshot.readByType(eventType).covary[F])

    private def follow(
        select: StoreState => Vector[EventPosition],
        from: Long
    ): Stream[F, StoredEvent] =
      // discrete may coalesce updates. Indices retain every committed event, so
      // the cursor catches up from each observed state without depending on notifications.
      state.discrete
        .mapAccumulate(from) { (cursor, snapshot) =>
          val positions = select(snapshot)
          val end = positions.size.toLong
          val start = cursor.min(end).toInt
          (cursor.max(end), snapshot.resolve(positions, start, positions.size))
        }
        .flatMap { case (_, events) => events.covary[F] }

    def listen(
        streamId: StreamId,
        fromRevision: Revision
    ): Stream[F, StoredEvent] =
      follow(_.streams.getOrElse(streamId, Vector.empty), fromRevision.value)

    def listenByType(eventType: EventType): Stream[F, StoredEvent] =
      follow(_.eventTypes.getOrElse(eventType, Vector.empty), 0L)
