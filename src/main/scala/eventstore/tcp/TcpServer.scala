package eventstore.tcp

import cats.MonadThrow
import cats.effect.{Concurrent, Resource}
import cats.syntax.all.*
import com.comcast.ip4s.GenSocketAddress
import eventstore.EventStore
import fs2.Stream
import fs2.io.net.{Network, Socket}

object TcpServer:
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

  private def serveConnection[F[_]: MonadThrow](
      store: EventStore[F],
      socket: Socket[F]
  ): Stream[F, Unit] =
    Stream
      .repeatEval(Framing.read(socket))
      .unNoneTerminate
      .flatMap(body => Protocol.responses(store, body))
      .evalMap(json => Framing.write(socket, Protocol.bytes(json)))
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
