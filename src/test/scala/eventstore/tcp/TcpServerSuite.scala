package eventstore.tcp

import cats.data.NonEmptyList
import cats.effect.{Deferred, IO, Resource}
import cats.effect.syntax.all.*
import cats.syntax.all.*
import com.comcast.ip4s.*
import eventstore.*
import fs2.{Chunk, Stream}
import fs2.io.net.{Network, Socket}
import io.circe.Json
import io.circe.parser.parse
import munit.CatsEffectSuite
import java.nio.charset.StandardCharsets.UTF_8
import scala.concurrent.duration.*

class TcpServerSuite extends CatsEffectSuite:
  override val munitIOTimeout = 15.seconds

  private def running(
      store: EventStore[IO],
      maxConnections: Int = 64
  ): Resource[IO, TcpServer.Bound[IO]] =
    TcpServer
      .bind[IO](
        store,
        SocketAddress(ip"127.0.0.1", port"0"),
        _ => IO.unit,
        maxConnections
      )
      .flatMap(server => server.run.background.as(server))

  private def connect(server: TcpServer.Bound[IO]): Resource[IO, Socket[IO]] =
    Network[IO].connect(server.address)

  private def send(socket: Socket[IO], json: String): IO[Unit] =
    Framing.write(socket, Chunk.array(json.getBytes(UTF_8)))

  private def receive(socket: Socket[IO]): IO[Json] =
    Framing.read(socket).flatMap {
      case Some(bytes) => IO.fromEither(parse(new String(bytes.toArray, UTF_8)))
      case None        => IO.raiseError(new RuntimeException("Unexpected EOF"))
    }

  private def response(socket: Socket[IO]): IO[List[Json]] =
    receive(socket).flatMap { frame =>
      if frame.hcursor.get[String]("type").toOption.contains("event") then
        response(socket).map(frame :: _)
      else IO.pure(List(frame))
    }

  private def kind(json: Json): String =
    json.hcursor.get[String]("type").toOption.get
  private def id(json: Json): String =
    json.hcursor.downField("event").get[String]("eventId").toOption.get

  private val append =
    """{"op":"append","streamId":"orders","expectedRevision":"noStream","events":[{"eventId":"a","eventType":"Placed","payloadBase64":"AAH/"},{"eventId":"b","eventType":"Paid","payloadBase64":""}]}"""

  test(
    "TCP append, fetch, bounded read, and type query share the store and connection"
  ) {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store).use { server =>
        connect(server).use { socket =>
          for
            _ <- send(socket, append)
            appended <- response(socket)
            _ <- send(socket, """{"op":"fetch","eventId":"a"}""")
            fetched <- response(socket)
            _ <- send(
              socket,
              """{"op":"read","streamId":"orders","fromRevision":1,"toRevision":1}"""
            )
            read <- response(socket)
            _ <- send(socket, """{"op":"readByType","eventType":"Placed"}""")
            byType <- response(socket)
            _ <- send(
              socket,
              """{"op":"fetch","streamId":"other","eventId":"a"}"""
            )
            absent <- response(socket)
          yield
            assertEquals(appended.map(kind), List("event", "event", "done"))
            assertEquals(appended.take(2).map(id), List("a", "b"))
            assertEquals(
              fetched.head.hcursor
                .downField("event")
                .get[String]("payloadBase64"),
              Right("AAH/")
            )
            assertEquals(read.map(kind), List("event", "done"))
            assertEquals(id(read.head), "b")
            assertEquals(id(byType.head), "a")
            assertEquals(absent.map(kind), List("done"))
        }
      }
    }
  }

  test(
    "malformed requests and domain errors return terminal errors and allow another request"
  ) {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store).use { server =>
        connect(server).use { socket =>
          val cases = List(
            "{" -> "invalid_request",
            """{"op":"unknown"}""" -> "invalid_request",
            """{"op":"append","streamId":"s","events":[]}""" -> "invalid_request",
            """{"op":"append","streamId":"s","events":[{"eventType":"T","payloadBase64":"!"}]}""" -> "invalid_request",
            """{"op":"read","streamId":"s","fromRevision":-1}""" -> "invalid_argument",
            """{"op":"read","streamId":" ","fromRevision":0}""" -> "invalid_argument",
            """{"op":"fetch","eventId":" "}""" -> "invalid_argument",
            """{"op":"fetch","eventId":"a","streamId":" "}""" -> "invalid_argument",
            """{"op":"readByType","eventType":" "}""" -> "invalid_argument",
            """{"op":"append","streamId":"s","expectedRevision":-1,"events":[{"eventType":"T","payloadBase64":""}]}""" -> "invalid_argument",
            """{"op":"append","streamId":"s","events":[{"eventId":"a","eventType":"T","payloadBase64":""},{"eventId":"b","eventType":" ","payloadBase64":""}]}""" -> "invalid_argument",
            """{"op":"append","streamId":"orders","expectedRevision":10,"events":[{"eventType":"T","payloadBase64":""}]}""" -> "revision_conflict"
          )
          for
            _ <- cases.traverse_ { (request, code) =>
              send(socket, request) *> receive(socket).flatMap { frame =>
                IO {
                  assertEquals(kind(frame), "error")
                  assertEquals(frame.hcursor.get[String]("code"), Right(code))
                }
              }
            }
            _ <- send(socket, append)
            _ <- response(socket)
            _ <- send(socket, append.replace("\"noStream\"", "\"any\""))
            duplicate <- receive(socket)
            _ <- send(socket, """{"op":"read","streamId":"orders"}""")
            read <- response(socket)
          yield
            assertEquals(
              duplicate.hcursor.get[String]("code"),
              Right("duplicate_event")
            )
            assertEquals(read.map(kind), List("event", "event", "done"))
        }
      }
    }
  }

  test(
    "framing handles split headers and bodies, coalesced requests, and half-close"
  ) {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store).use { server =>
        connect(server).use { socket =>
          val body = Chunk.array(
            """{"op":"readByType","eventType":"未登録"}""".getBytes(UTF_8)
          )
          val header = Chunk[Byte](0, 0, 0, body.size.toByte)
          for
            _ <- socket.write(header.take(1))
            _ <- socket.write(header.drop(1) ++ body.take(5))
            _ <- socket.write(body.drop(5) ++ header ++ body)
            _ <- socket.endOfOutput
            first <- receive(socket)
            second <- receive(socket)
            eof <- Framing.read(socket)
          yield
            assertEquals(kind(first), "done")
            assertEquals(kind(second), "done")
            assertEquals(eof, None)
        }
      }
    }
  }

  test("invalid and truncated frames close only their connection") {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store).use { server =>
        val badFrames = List(
          Chunk[Byte](0, 0, 0, 0),
          Chunk[Byte](-1, -1, -1, -1),
          Chunk[Byte](0, 16, 0, 1), // 1 MiB + 1
          Chunk[Byte](0, 0),
          Chunk[Byte](0, 0, 0, 5, 123)
        )
        badFrames.traverse_ { bytes =>
          connect(server).use { socket =>
            for
              _ <- socket.write(bytes)
              _ <- socket.endOfOutput
              error <- receive(socket)
              eof <- Framing.read(socket)
            yield
              assertEquals(
                error.hcursor.get[String]("code"),
                Right("invalid_frame")
              )
              assertEquals(eof, None)
          }
        } *> connect(server).use { socket =>
          send(socket, """{"op":"readByType","eventType":"T"}""") *>
            receive(socket).map(frame => assertEquals(kind(frame), "done"))
        }
      }
    }
  }

  test(
    "one event reaches the client before the rest of the query is available"
  ) {
    for
      store <- InMemoryEventStore.create[IO]
      _ <- store.append(
        StreamId.unsafeFrom("s"),
        NonEmptyList.of(
          NewEvent(
            Chunk.empty,
            EventType.unsafeFrom("T"),
            Some(EventId.unsafeFrom("a"))
          ),
          NewEvent(
            Chunk.empty,
            EventType.unsafeFrom("T"),
            Some(EventId.unsafeFrom("b"))
          )
        )
      )
      release <- Deferred[IO, Unit]
      streaming = new EventStore[IO]:
        def append(
            s: StreamId,
            e: NonEmptyList[NewEvent],
            r: ExpectedRevision
        ) = store.append(s, e, r)
        def fetch(s: StreamId, e: EventId) = store.fetch(s, e)
        def fetch(e: EventId) = store.fetch(e)
        def readByType(t: EventType) = store.readByType(t)
        def listen(s: StreamId, from: Revision) = store.listen(s, from)
        def listenByType(t: EventType) = store.listenByType(t)
        def read(
            s: StreamId,
            from: Revision,
            to: Option[Revision]
        ): Stream[IO, StoredEvent] =
          store
            .read(s, from, to)
            .evalTap(e =>
              if e.revision.value == 1 then release.get else IO.unit
            )
      _ <- running(streaming).use { server =>
        connect(server).use { socket =>
          for
            _ <- send(socket, """{"op":"read","streamId":"s"}""")
            first <- receive(socket)
            _ <- IO(assertEquals(id(first), "a"))
            _ <- release.complete(())
            rest <- response(socket)
          yield assertEquals(rest.map(kind), List("event", "done"))
        }
      }
    yield ()
  }

  test("a stalled connection does not prevent other clients from querying") {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store).use { server =>
        connect(server).use { stalled =>
          stalled
            .write(Chunk[Byte](0)) *> (0 until 10).toList.parTraverse_ { _ =>
            connect(server).use { socket =>
              send(socket, """{"op":"readByType","eventType":"T"}""") *>
                receive(socket).map(frame => assertEquals(kind(frame), "done"))
            }
          }
        }
      }
    }
  }

  test(
    "TCP listeners replay history then receive live events by stream and type"
  ) {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store).use { server =>
        (connect(server), connect(server), connect(server)).tupled.use {
          case (streamListener, typeListener, writer) =>
            for
              _ <- send(writer, append)
              _ <- response(writer)
              _ <- send(
                streamListener,
                """{"op":"listen","streamId":"orders","fromRevision":1}"""
              )
              readyStream <- receive(streamListener)
              replayStream <- receive(streamListener)
              _ <- send(
                typeListener,
                """{"op":"listenByType","eventType":"Placed"}"""
              )
              readyType <- receive(typeListener)
              replayType <- receive(typeListener)
              _ <- send(
                writer,
                """{"op":"append","streamId":"other","events":[{"eventId":"c","eventType":"Placed","payloadBase64":""}]}"""
              )
              _ <- response(writer)
              _ <- send(
                writer,
                """{"op":"append","streamId":"orders","events":[{"eventId":"d","eventType":"Paid","payloadBase64":""}]}"""
              )
              _ <- response(writer)
              liveStream <- receive(streamListener)
              liveType <- receive(typeListener)
            yield
              assertEquals(kind(readyStream), "subscribed")
              assertEquals(kind(readyType), "subscribed")
              assertEquals(id(replayStream), "b")
              assertEquals(id(replayType), "a")
              assertEquals(id(liveStream), "d")
              assertEquals(id(liveType), "c")
        }
      }
    }
  }

  test("disconnecting an idle subscription releases its connection slot") {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store, maxConnections = 1).use { server =>
        connect(server).use { socket =>
          send(socket, """{"op":"listen","streamId":"not-created-yet"}""") *>
            receive(socket)
              .map(frame => assertEquals(kind(frame), "subscribed"))
        } *> connect(server).use { socket =>
          send(socket, """{"op":"readByType","eventType":"T"}""") *>
            receive(socket).map(frame => assertEquals(kind(frame), "done"))
        }
      }
    }
  }

  test(
    "subscriptions reject further commands and invalid subscription arguments"
  ) {
    InMemoryEventStore.create[IO].flatMap { store =>
      running(store).use { server =>
        connect(server).use { socket =>
          for
            _ <- send(socket, """{"op":"listenByType","eventType":"T"}""")
            ready <- receive(socket)
            _ <- send(socket, """{"op":"readByType","eventType":"T"}""")
            error <- receive(socket)
            eof <- Framing.read(socket)
          yield
            assertEquals(kind(ready), "subscribed")
            assertEquals(
              error.hcursor.get[String]("code"),
              Right("invalid_request")
            )
            assertEquals(eof, None)
        } *> List(
          """{"op":"listen","streamId":"s","fromRevision":-1}""",
          """{"op":"listen","streamId":" "}""",
          """{"op":"listenByType","eventType":" "}"""
        ).traverse_ { request =>
          connect(server).use { socket =>
            send(socket, request) *> receive(socket).map { frame =>
              assertEquals(
                frame.hcursor.get[String]("code"),
                Right("invalid_argument")
              )
            }
          }
        }
      }
    }
  }
