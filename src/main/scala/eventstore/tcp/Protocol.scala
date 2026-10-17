package eventstore.tcp

import cats.data.NonEmptyList
import cats.MonadThrow
import cats.syntax.all.*
import eventstore.*
import fs2.{Chunk, Stream}
import io.circe.{Decoder, DecodingFailure, HCursor, Json}
import io.circe.parser.decode
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Base64

/** JSON wire format lives entirely in the TCP adapter. */
object Protocol:
  private final case class InputEvent(
      payload: Chunk[Byte],
      eventType: String,
      eventId: Option[String]
  ):
    def toDomain: Either[StoreError, NewEvent] =
      for
        kind <- EventType.from(eventType)
        id <- eventId.traverse(EventId.from)
      yield NewEvent(payload, kind, id)

  private enum InputExpectedRevision:
    case Any, NoStream
    case Exact(revision: Long)

    def toDomain: Either[StoreError, ExpectedRevision] = this match
      case Any      => Right(ExpectedRevision.Any)
      case NoStream => Right(ExpectedRevision.NoStream)
      case Exact(n) => Revision.from(n).map(ExpectedRevision.Exact.apply)

  private enum Request:
    case Append(
        streamId: String,
        events: NonEmptyList[InputEvent],
        expected: InputExpectedRevision
    )
    case Fetch(eventId: String, streamId: Option[String])
    case Read(streamId: String, from: Long, to: Option[Long])
    case ReadByType(eventType: String)
    case Listen(streamId: String, from: Long)
    case ListenByType(eventType: String)

  private def failure(cursor: HCursor, message: String): DecodingFailure =
    DecodingFailure(message, cursor.history)

  private given Decoder[InputEvent] = Decoder.instance { cursor =>
    for
      eventType <- cursor.get[String]("eventType")
      payload <- cursor.get[String]("payloadBase64")
      bytes <- Either
        .catchNonFatal(Base64.getDecoder.decode(payload))
        .leftMap(_ => failure(cursor, "Invalid base64 payload"))
      id <- cursor.get[Option[String]]("eventId")
    yield InputEvent(Chunk.array(bytes), eventType, id)
  }

  private def expected(cursor: HCursor): Decoder.Result[InputExpectedRevision] =
    cursor.get[Option[Json]]("expectedRevision").flatMap {
      case None => Right(InputExpectedRevision.Any)
      case Some(json) if json.asString.contains("any") =>
        Right(InputExpectedRevision.Any)
      case Some(json) if json.asString.contains("noStream") =>
        Right(InputExpectedRevision.NoStream)
      case Some(json) =>
        json.asNumber
          .flatMap(_.toLong)
          .map(n => InputExpectedRevision.Exact(n))
          .toRight(
            failure(
              cursor,
              "expectedRevision must be any, noStream, or an integer"
            )
          )
    }

  private given Decoder[Request] = Decoder.instance { cursor =>
    cursor.get[String]("op").flatMap {
      case "append" =>
        for
          streamId <- cursor.get[String]("streamId")
          events <- cursor.get[List[InputEvent]]("events")
          batch <- NonEmptyList
            .fromList(events)
            .toRight(failure(cursor, "events must be nonempty"))
          revision <- expected(cursor)
        yield Request.Append(streamId, batch, revision)
      case "fetch" =>
        for
          id <- cursor.get[String]("eventId")
          streamId <- cursor.get[Option[String]]("streamId")
        yield Request.Fetch(id, streamId)
      case "read" =>
        for
          streamId <- cursor.get[String]("streamId")
          from <- cursor.get[Option[Long]]("fromRevision")
          to <- cursor.get[Option[Long]]("toRevision")
        yield Request.Read(streamId, from.getOrElse(0L), to)
      case "readByType" =>
        cursor.get[String]("eventType").map(Request.ReadByType.apply)
      case "listen" =>
        for
          streamId <- cursor.get[String]("streamId")
          from <- cursor.get[Option[Long]]("fromRevision")
        yield Request.Listen(streamId, from.getOrElse(0L))
      case "listenByType" =>
        cursor.get[String]("eventType").map(Request.ListenByType.apply)
      case other => Left(failure(cursor, s"Unknown operation: $other"))
    }
  }

  def bytes(json: Json): Chunk[Byte] =
    Chunk.array(json.noSpaces.getBytes(UTF_8))

  def error(code: String, message: String): Json = Json.obj(
    "type" -> Json.fromString("error"),
    "code" -> Json.fromString(code),
    "message" -> Json.fromString(message)
  )

  private def eventJson(event: StoredEvent): Json = Json.obj(
    "type" -> Json.fromString("event"),
    "event" -> Json.obj(
      "streamId" -> Json.fromString(event.streamId.value),
      "eventId" -> Json.fromString(event.eventId.value),
      "eventType" -> Json.fromString(event.eventType.value),
      "revision" -> Json.fromLong(event.revision.value),
      "position" -> Json.fromLong(event.position.value),
      "payloadBase64" -> Json.fromString(
        Base64.getEncoder.encodeToString(event.payload.toArray)
      )
    )
  )

  final case class Response[F[_]](frames: Stream[F, Json], listening: Boolean)

  private val subscribed: Json =
    Json.obj("type" -> Json.fromString("subscribed"))

  private def finite[F[_]](events: Stream[F, StoredEvent]): Stream[F, Json] =
    events.map(eventJson) ++ Stream.emit(
      Json.obj("type" -> Json.fromString("done"))
    )

  /** Finite operations end in done; listeners stay open until cancelled or
    * failed.
    */
  def response[F[_]: MonadThrow](
      store: EventStore[F],
      body: Chunk[Byte]
  ): Response[F] =
    decode[Request](new String(body.toArray, UTF_8)) match
      case Left(problem) =>
        Response(
          Stream.emit(error("invalid_request", problem.getMessage)),
          false
        )
      case Right(request) =>
        val listening = request match
          case Request.Listen(_, _) | Request.ListenByType(_) => true
          case _                                              => false
        val frames = request match
          case Request.Append(id, batch, revision) =>
            val validated = for
              streamId <- StreamId.from(id)
              domainEvents <- batch.traverse(_.toDomain)
              expected <- revision.toDomain
            yield (streamId, domainEvents, expected)
            finite(
              Stream
                .eval(MonadThrow[F].fromEither(validated).flatMap {
                  case (streamId, domainEvents, expected) =>
                    store.append(streamId, domainEvents, expected)
                })
                .flatMap(es => Stream.emits(es.toList))
            )
          case Request.Fetch(id, streamId) =>
            val validated = for
              eventId <- EventId.from(id)
              owner <- streamId.traverse(StreamId.from)
            yield (eventId, owner)
            finite(
              Stream
                .eval(MonadThrow[F].fromEither(validated).flatMap {
                  case (eventId, owner) =>
                    owner.fold(store.fetch(eventId))(store.fetch(_, eventId))
                })
                .flatMap(e => Stream.emits(e.toList))
            )
          case Request.Read(id, from, to) =>
            val validated = for
              streamId <- StreamId.from(id)
              first <- Revision.from(from)
              last <- to.traverse(Revision.from)
            yield (streamId, first, last)
            finite(Stream.eval(MonadThrow[F].fromEither(validated)).flatMap {
              case (streamId, first, last) => store.read(streamId, first, last)
            })
          case Request.ReadByType(kind) =>
            finite(
              Stream
                .eval(MonadThrow[F].fromEither(EventType.from(kind)))
                .flatMap(store.readByType)
            )
          case Request.Listen(id, from) =>
            val validated = for
              streamId <- StreamId.from(id)
              first <- Revision.from(from)
            yield (streamId, first)
            Stream.eval(MonadThrow[F].fromEither(validated)).flatMap {
              case (streamId, first) =>
                Stream.emit(subscribed) ++ store
                  .listen(streamId, first)
                  .map(eventJson)
            }
          case Request.ListenByType(kind) =>
            Stream
              .eval(MonadThrow[F].fromEither(EventType.from(kind)))
              .flatMap { eventType =>
                Stream.emit(subscribed) ++ store
                  .listenByType(eventType)
                  .map(eventJson)
              }
        Response(
          frames.handleErrorWith {
            case e: StoreError =>
              val code = e match
                case _: StoreError.InvalidArgument  => "invalid_argument"
                case _: StoreError.DuplicateEvent   => "duplicate_event"
                case _: StoreError.RevisionConflict => "revision_conflict"
              Stream.emit(error(code, e.getMessage))
            case other => Stream.raiseError[F](other)
          },
          listening
        )
