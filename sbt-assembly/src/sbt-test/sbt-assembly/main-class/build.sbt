lazy val root = (project in file(".")).
  settings(
    version := "0.1",
    scalaVersion := "2.10.2",
    libraryDependencies += "net.databinder.dispatch" %% "dispatch-core" % "0.11.2",
    mainClass in assembly := Some("foo.Hello"),
    assemblyJarName in assembly := "foo.jar",
    TaskKey[Unit]("check") <<= (crossTarget) map { (crossTarget) =>
      val process = sbt.Process("java", Seq("-jar", (crossTarget / "foo.jar").toString))
      val out = (process!!)
      if (out.trim != "hello") error("unexpected output: " + out)
      ()
    }
  )
