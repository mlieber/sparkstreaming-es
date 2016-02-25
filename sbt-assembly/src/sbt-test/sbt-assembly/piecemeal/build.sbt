lazy val root = (project in file(".")).
  settings(
    name := "foo",
    version := "0.1",
    scalaVersion := "2.10.2",
    assembleArtifact in assemblyPackageScala := false,
    assembleArtifact in assemblyPackageDependency := false,
    TaskKey[Unit]("check") <<= (crossTarget) map { (crossTarget) =>
      val process = sbt.Process("java", Seq("-cp", 
        (crossTarget / "scala-library-2.10.2-assembly.jar").toString + ":" +
        (crossTarget / "foo-assembly-0.1.jar").toString,
        "Main"))
      val out = (process!!)
      if (out.trim != "hello") error("unexpected output: " + out)
      ()
    }
  )
