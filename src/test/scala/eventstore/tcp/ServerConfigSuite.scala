package eventstore.tcp

import com.comcast.ip4s.*

class ServerConfigSuite extends munit.FunSuite:
  test("defaults bind to loopback on port 5252") {
    assertEquals(ServerConfig.parse(Nil), Right(Some(ServerConfig())))
    assertEquals(
      ServerConfig().address,
      SocketAddress(ip"127.0.0.1", port"5252")
    )
  }

  test("host and port can be overridden independently and in either order") {
    assertEquals(
      ServerConfig.parse(List("--host", "0.0.0.0")),
      Right(Some(ServerConfig(host = ip"0.0.0.0")))
    )
    assertEquals(
      ServerConfig.parse(List("--port", "6000")),
      Right(Some(ServerConfig(port = port"6000")))
    )
    val expected =
      Right(Some(ServerConfig(host = ip"0.0.0.0", port = port"6000")))
    assertEquals(
      ServerConfig.parse(List("--host", "0.0.0.0", "--port", "6000")),
      expected
    )
    assertEquals(
      ServerConfig.parse(List("--port", "6000", "--host", "0.0.0.0")),
      expected
    )
  }

  test("hostnames, IPv6, and ephemeral ports are supported") {
    assertEquals(
      ServerConfig.parse(List("--host", "localhost", "--port", "0")),
      Right(Some(ServerConfig(host = host"localhost", port = port"0")))
    )
    assertEquals(
      ServerConfig.parse(List("--host", "::1", "--port", "65535")),
      Right(Some(ServerConfig(host = ip"::1", port = port"65535")))
    )
  }

  test("help does not request server startup") {
    assertEquals(ServerConfig.parse(List("--help")), Right(None))
    assertEquals(ServerConfig.parse(List("-h")), Right(None))
  }

  test("malformed arguments are rejected") {
    List(
      List("--host"),
      List("--port"),
      List("--host", ""),
      List("--host", "bad host"),
      List("--port", "abc"),
      List("--port", "-1"),
      List("--port", "65536"),
      List("--port", "9999999999999999"),
      List("--host", "localhost", "--host", "127.0.0.1"),
      List("--port", "5252", "--port", "6000"),
      List("--unknown"),
      List("5252")
    ).foreach(args => assert(ServerConfig.parse(args).isLeft, clues(args)))
  }
