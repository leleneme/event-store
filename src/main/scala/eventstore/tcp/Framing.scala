package eventstore.tcp

import cats.MonadThrow
import cats.syntax.all.*
import fs2.Chunk
import fs2.io.net.Socket

/** Four-byte big-endian length followed by that many bytes. */
object Framing:
  val MaxRequestBytes: Int = 1024 * 1024
  final case class InvalidFrame(reason: String) extends RuntimeException(reason)

  def read[F[_]: MonadThrow](socket: Socket[F]): F[Option[Chunk[Byte]]] =
    socket.readN(4).flatMap { header =>
      if header.isEmpty then none[Chunk[Byte]].pure[F]
      else if header.size != 4 then
        MonadThrow[F].raiseError(InvalidFrame("Truncated frame header"))
      else
        val size = header.foldLeft(0)((n, byte) => (n << 8) | (byte & 0xff))
        if size <= 0 || size > MaxRequestBytes then
          MonadThrow[F].raiseError(
            InvalidFrame(
              s"Frame length must be between 1 and $MaxRequestBytes bytes"
            )
          )
        else
          socket.readN(size).flatMap { body =>
            if body.size == size then body.some.pure[F]
            else MonadThrow[F].raiseError(InvalidFrame("Truncated frame body"))
          }
    }

  def write[F[_]](socket: Socket[F], bytes: Chunk[Byte]): F[Unit] =
    val size = bytes.size
    val header = Chunk[Byte](
      (size >>> 24).toByte,
      (size >>> 16).toByte,
      (size >>> 8).toByte,
      size.toByte
    )
    socket.write(header ++ bytes)
