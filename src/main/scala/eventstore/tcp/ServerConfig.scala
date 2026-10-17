package eventstore.tcp

import com.comcast.ip4s.*
import scala.annotation.tailrec

final case class ServerConfig(
    host: Host = ip"127.0.0.1",
    port: Port = port"5252"
):
  def address: SocketAddress[Host] = SocketAddress(host, port)

object ServerConfig:
  val usage: String =
    """Usage: sc-event-store [--host HOST] [--port PORT]
      |
      |  --host HOST  Bind address or hostname (default: 127.0.0.1)
      |  --port PORT  TCP port, 0-65535 (default: 5252; 0 selects a free port)
      |  -h, --help   Show this help
      |""".stripMargin

  /** None requests help; malformed arguments are reported before opening
    * sockets.
    */
  def parse(args: List[String]): Either[String, Option[ServerConfig]] =
    @tailrec
    def loop(
        remaining: List[String],
        config: ServerConfig,
        seen: Set[String]
    ): Either[String, Option[ServerConfig]] = remaining match
      case Nil                     => Right(Some(config))
      case ("--help" | "-h") :: _  => Right(None)
      case flag :: _ if seen(flag) => Left(s"Duplicate option: $flag")
      case (flag @ ("--host" | "--port")) :: Nil =>
        Left(s"Missing value for $flag")
      case "--host" :: value :: tail =>
        Host.fromString(value) match
          case None       => Left(s"Invalid host: $value")
          case Some(host) =>
            loop(tail, config.copy(host = host), seen + "--host")
      case "--port" :: value :: tail =>
        Port.fromString(value) match
          case None =>
            Left(s"Invalid port: $value (expected an integer from 0 to 65535)")
          case Some(port) =>
            loop(tail, config.copy(port = port), seen + "--port")
      case unknown :: _ => Left(s"Unknown argument: $unknown")

    loop(args, ServerConfig(), Set.empty)
