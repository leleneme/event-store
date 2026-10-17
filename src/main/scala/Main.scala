import cats.effect.{ExitCode, IO, IOApp}
import cats.effect.std.Console
import eventstore.InMemoryEventStore
import eventstore.tcp.{ServerConfig, TcpServer}

object Main extends IOApp:
  def run(args: List[String]): IO[ExitCode] =
    ServerConfig.parse(args) match
      case Left(error) =>
        Console[IO]
          .errorln(s"$error\n\n${ServerConfig.usage}")
          .as(ExitCode.Error)
      case Right(None) => IO.println(ServerConfig.usage).as(ExitCode.Success)
      case Right(Some(config)) =>
        InMemoryEventStore
          .create[IO]
          .flatMap { store =>
            TcpServer
              .bind[IO](
                store,
                config.address,
                error =>
                  Console[IO]
                    .errorln(s"TCP connection failed: ${error.getMessage}")
              )
              .use { server =>
                IO.println(
                  s"Event store listening on ${server.address}"
                ) *> server.run
              }
          }
          .as(ExitCode.Success)
