lazy val testmerge = (project in file(".")).
  settings(
    version := "0.1",
    jarName in assembly := "foo.jar",
    mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
        case _ => MergeStrategy.singleOrError
      }
    }
  )
