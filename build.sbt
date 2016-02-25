import sbtassembly.AssemblyKeys
name := "stream-test"

version := "1.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka" % "1.6.0" 
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "1.6.0" 
libraryDependencies += "org.json4s" %% "json4s-native" % "3.2.10"
libraryDependencies += "org.elasticsearch" % "elasticsearch" % "1.7.2" % "provided"
libraryDependencies += "org.elasticsearch" %% "elasticsearch-spark" % "2.1.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "2.2.4" % "test"
    
assemblyJarName in assembly := "stream_test_" + version.value + ".jar"

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

