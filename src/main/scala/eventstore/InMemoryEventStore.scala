package eventstore

import cats.MonadThrow
import cats.data.NonEmptyList
import cats.effect.{Ref, Sync}
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.all.*
import fs2.Stream

object InMemoryEventStore:
  // Allocating the secure random source requires Sync; operating the store does not.
  def create[F[_]: Sync]: F[EventStore[F]] =
    SecureRandom.of[F].flatMap { random =>
      given SecureRandom[F] = random
      Ref.of[F, StoreState](StoreState()).map(new Impl[F](_))
    }

  private final class Impl[F[_]: MonadThrow: UUIDGen](state: Ref[F, StoreState])
      extends EventStore[F]:
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
