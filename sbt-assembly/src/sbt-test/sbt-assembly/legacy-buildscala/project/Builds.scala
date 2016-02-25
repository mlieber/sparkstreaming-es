import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._

object Builds extends Build {
  lazy val commonSettings = Seq(
    version := "0.1-SNAPSHOT",
    organization := "com.example",
    scalaVersion := "2.10.1"
  )

  lazy val app = (project in file("app")).
    settings(commonSettings: _*).
    settings(
      TaskKey[Unit]("check") := {
        val process = sbt.Process("java", Seq("-jar", (crossTarget.value / "app-assembly-0.1-SNAPSHOT.jar").toString))
        val out = (process!!)
        if (out.trim != "hello") error("unexpected output: " + out)
        ()
      }
    )
}
