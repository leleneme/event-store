val scala3Version = "3.10.0"

lazy val root = project
  .in(file("."))
  .settings(
    name := "sc-event-store",
    version := "0.1.0-SNAPSHOT",

    scalaVersion := scala3Version,

    libraryDependencies ++= Seq(
      "org.typelevel" %% "cats-effect" % "3.7.1",
      "co.fs2" %% "fs2-core" % "3.14.0",
      "co.fs2" %% "fs2-io" % "3.14.0",
      "io.circe" %% "circe-parser" % "0.14.17",
      "org.typelevel" %% "munit-cats-effect" % "2.2.1" % Test
    )
  )
