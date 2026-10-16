package eventstore.tcp

import cats.effect.{IO, IOApp}
import com.comcast.ip4s.*
import eventstore.InMemoryEventStore

object ServerMain extends IOApp.Simple:
  def run: IO[Unit] =
    InMemoryEventStore.create[IO].flatMap { store =>
      TcpServer
        .bind[IO](
          store,
          SocketAddress(ip"127.0.0.1", port"5252"),
          error => IO.println(s"TCP connection failed: ${error.getMessage}")
        )
        .use(server =>
          IO.println(
            s"Event store listening on ${server.address}"
          ) *> server.run
        )
    }
