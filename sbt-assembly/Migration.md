  [pbp]: http://www.scala-sbt.org/0.13/docs/Plugins-Best-Practices.html
  [1]: https://github.com/sbt/sbt-assembly/blob/0.12.0/src/main/scala/sbtassembly/AssemblyKeys.scala

Upgrading from 0.11.x to 0.12.0
-------------------------------

### sbt 0.13.5 or above

Auto plugins are available only for sbt 0.13.5 and above.

### Some key names have changed

In accordance to revised [plugin best practice][pbp] guide, all keys introduced by sbt-assembly are now prefixed with "assembly" or "assemble."
So, for example `jarName` becomes `assemblyJarName`, and `mergeStrategy` becomes `assemblyMergeStrategy`. For easier migration, the older key names are deprecated but still kept in the plugin for 0.12.0. See [AssemblyKeys.scala][1]

### Upgrading with multi-project build.sbt

If you are using multi-project `build.sbt` (before):

```scala
import AssemblyKeys._

lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.example",
  scalaVersion := "2.10.1"
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(assemblySettings: _*).
  settings(
    // your settings here
  )
```

1. Remove `import AssemblyKeys._`. The keys are now auto imported.
2. Remove `settings(assemblySettings: _*).`. The settings are now auto injected to all projects with `JvmPlugin`.

Here's how build.sbt looks now:

```scala
lazy val commonSettings = Seq(
  version := "0.1-SNAPSHOT",
  organization := "com.example",
  scalaVersion := "2.10.1"
)

lazy val app = (project in file("app")).
  settings(commonSettings: _*).
  settings(
    // your settings here
  )
```

### Upgrading with bare build.sbt

Here's how `assembly.sbt` at the root directory would've looked (before):

```scala
import AssemblyKeys._ // put this at the top of the file

assemblySettings

// your assembly settings here
```

1. Remove `import AssemblyKeys._`. The keys are now auto imported.
2. Remove `assemblySettings`. The settings are now auto injected to all projects with `JvmPlugin`.

Here's how `assembly.sbt` now looks:

```scala
// your assembly settings here
```

In other words, we no longer need `assembly.sbt` unless you need additional settings.

### Upgrading with build.scala

Here's how `build.scala` would've looked (before):

```scala
import sbt._
import Keys._
import sbtassembly.Plugin._
import AssemblyKeys._

object Builds extends Build {
  lazy val commonSettings = Defaults.defaultSettings ++ Seq(
    version := "0.1-SNAPSHOT",
    organization := "com.example",
    scalaVersion := "2.10.1"
  )

  lazy val app = Project("app", file("app"),
    settings = commonSettings ++ assemblySettings) settings(
      // your settings here
    )
}
```

The recommended route of upgrade is to go to multi-project `build.sbt`.
If you want to stay on `build.scala` for whatever reason, it would look like multi-project `build.sbt` with `object` around it.

1. Replace imports of `sbtassembly.Plugin._` and `AssemblyKeys._` with `sbtassembly.AssemblyPlugin.autoImport._`.
2. Drop `Defaults.defaultSettings` from commonSettings.
3. Use `settings()` method to append `commonSettings`.

```scala
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
      // your settings here
    )
}
```
