lazy val testshade = (project in file(".")).
  settings(
    version := "0.1",
    assemblyJarName in assembly := "foo.jar",
    scalaVersion := "2.10.5",
    libraryDependencies += "commons-io" % "commons-io" % "2.4",
    assemblyShadeRules in assembly := Seq(
      ShadeRule.zap("remove.**").inProject,
      ShadeRule.rename("toshade.ShadeClass" -> "toshade.ShadedClass").inProject,
      ShadeRule.rename("toshade.ShadePackage" -> "shaded_package.ShadePackage").inProject,
      ShadeRule.rename("org.apache.commons.io.**" -> "shadeio.@1").inLibrary("commons-io" % "commons-io" % "2.4").inProject
    ),
    // logLevel in assembly := Level.Debug,
    TaskKey[Unit]("check") <<= (crossTarget) map { (crossTarget) ⇒
      IO.withTemporaryDirectory { dir ⇒
        IO.unzip(crossTarget / "foo.jar", dir)
        mustNotExist(dir / "remove" / "Removed.class")
        mustNotExist(dir / "org" / "apache" / "commons" / "io" / "ByteOrderMark.class")
        mustExist(dir / "shaded_package" / "ShadePackage.class")
        mustExist(dir / "toshade" / "ShadedClass.class")
        mustExist(dir / "shadeio" / "ByteOrderMark.class")
      }
      val process = sbt.Process("java", Seq("-jar", (crossTarget / "foo.jar").toString))
      val out = (process!!)
      if (out.trim != "hello shadeio.filefilter.AgeFileFilter") error("unexpected output: " + out)
      ()
    })

def mustNotExist(f: File): Unit = {
  if (f.exists) sys.error("file" + f + " exists!")
}
def mustExist(f: File): Unit = {
  if (!f.exists) sys.error("file" + f + " does not exist!")
}
