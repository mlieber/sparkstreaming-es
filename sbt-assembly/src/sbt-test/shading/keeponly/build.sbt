lazy val testkeep = (project in file(".")).
  settings(
    version := "0.1",
    assemblyJarName in assembly := "foo.jar",
    scalaVersion := "2.9.1",
    assemblyShadeRules in assembly := Seq(
      ShadeRule.keep("keep.**").inProject
    ),
    TaskKey[Unit]("check") <<= (crossTarget) map { (crossTarget) ⇒
      IO.withTemporaryDirectory { dir ⇒
        IO.unzip(crossTarget / "foo.jar", dir)
        mustNotExist(dir / "removed" / "ShadeClass.class")
        mustNotExist(dir / "removed" / "ShadePackage.class")
        mustExist(dir / "keep" / "Keeped.class")
      }
    })

def mustNotExist(f: File): Unit = {
  if (f.exists) sys.error("file" + f + " exists!")
}
def mustExist(f: File): Unit = {
  if (!f.exists) sys.error("file" + f + " does not exist!")
}
