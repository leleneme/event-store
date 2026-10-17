package eventstore.tcp

import cats.effect.{Concurrent, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.GenSocketAddress
import eventstore.EventStore
import fs2.Stream
import fs2.io.net.{Network, Socket}

object TcpServer:
  private final class SubscriptionInput
      extends RuntimeException("A subscription requires a dedicated connection")
  final case class Bound[F[_]](address: GenSocketAddress, run: F[Unit])

  /** Binding is scoped; cancelling run closes active clients before releasing
    * the listener.
    */
  def bind[F[_]: Concurrent: Network](
      store: EventStore[F],
      address: GenSocketAddress,
      onConnectionError: Throwable => F[Unit],
      maxConnections: Int = 64
  ): Resource[F, Bound[F]] =
    Resource.eval(
      Concurrent[F].raiseWhen(maxConnections <= 0)(
        new IllegalArgumentException("maxConnections must be positive")
      )
    ) *> Network[F].bind(address).map { listener =>
      val run = listener.accept
        .map { socket =>
          serveConnection(store, socket)
            .handleErrorWith(error => Stream.eval(onConnectionError(error)))
        }
        .parJoin(maxConnections)
        .compile
        .drain
      Bound(listener.address, run)
    }

  private def serveConnection[F[_]: Concurrent](
      store: EventStore[F],
      socket: Socket[F]
  ): Stream[F, Unit] =
    def write(json: io.circe.Json): F[Unit] =
      Framing.write(socket, Protocol.bytes(json))

    def loop: Stream[F, Unit] =
      Stream.eval(Framing.read(socket)).flatMap {
        case None       => Stream.empty
        case Some(body) =>
          val response = Protocol.response(store, body)
          val output = response.frames.evalMap(write)
          if response.listening then
            // Listening owns this connection. Monitor its input while waiting for
            // store updates, so even an idle disconnected listener is released.
            val disconnected: F[Either[Throwable, Unit]] = socket.read(1).map {
              case None    => Right(())
              case Some(_) => Left(new SubscriptionInput)
            }
            output.interruptWhen(disconnected).handleErrorWith {
              case error: SubscriptionInput =>
                Stream.eval(
                  write(Protocol.error("invalid_request", error.getMessage))
                )
              case other => Stream.raiseError[F](other)
            }
          else output ++ Stream.suspend(loop)
      }
    loop
      .handleErrorWith {
        case error: Framing.InvalidFrame =>
          Stream.eval(
            Framing.write(
              socket,
              Protocol.bytes(Protocol.error("invalid_frame", error.getMessage))
            )
          )
        case other => Stream.raiseError[F](other)
      }
