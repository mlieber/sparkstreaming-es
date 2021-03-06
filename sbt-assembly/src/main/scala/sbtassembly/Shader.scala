package sbtassembly

import java.io.File

import org.pantsbuild.jarjar.ext_util.EntryStruct
import org.pantsbuild.jarjar._

import sbt._

case class ShadeRule(shadePattern: ShadePattern, targets: Seq[ShadeTarget] = Seq()) {
  def inLibrary(moduleId: ModuleID*): ShadeRule =
    this.copy(targets = targets ++ (moduleId.toSeq map { x => ShadeTarget(moduleID = Some(x)) }))
  def inAll: ShadeRule =
    this.copy(targets = targets :+ ShadeTarget(inAll = true))
  def inProject: ShadeRule =
    this.copy(targets = targets :+ ShadeTarget(inProject = true))

  private[sbtassembly] def isApplicableTo(mod: ModuleID): Boolean =
    targets.exists(_.isApplicableTo(mod))

  private[sbtassembly] def isApplicableToCompiling: Boolean =
    targets.exists(_.inAll) || targets.exists(_.inProject)

}

sealed trait ShadePattern {
  def inLibrary(moduleId: ModuleID*): ShadeRule =
    ShadeRule(this, moduleId.toSeq map { x => ShadeTarget(moduleID = Some(x)) })
  def inAll: ShadeRule =
    ShadeRule(this, Seq(ShadeTarget(inAll = true)))
  def inProject: ShadeRule =
    ShadeRule(this, Seq(ShadeTarget(inProject = true)))
}

object ShadeRule {
  def rename(patterns: (String, String)*): ShadePattern = Rename(patterns.toSeq.toList)
  def zap(patterns: String*): ShadePattern = Zap(patterns.toSeq.toList)
  def keep(patterns: String*): ShadePattern = Keep(patterns.toSeq.toList)
  private[sbtassembly] case class Rename(patterns: List[(String, String)]) extends ShadePattern
  private[sbtassembly] case class Zap(patterns: List[String]) extends ShadePattern
  private[sbtassembly] case class Keep(patterns: List[String]) extends ShadePattern
}

private[sbtassembly] case class ShadeTarget(
  inAll: Boolean = false,
  inProject: Boolean = false,
  moduleID: Option[ModuleID] = None) {

  private[sbtassembly] def isApplicableTo(mod: ModuleID): Boolean =
    inAll || (moduleID match {
      case Some(m) if (m.organization == mod.organization) && (m.name == mod.name) && (m.revision == mod.revision) =>
        true
      case _ =>
        false
    })
}

private[sbtassembly] object Shader {
  def shadeDirectory(rules: Seq[ShadeRule], dir: File, log: Logger, level: Level.Value): Unit = {
    val jjrules = rules flatMap { r => r.shadePattern match {
      case ShadeRule.Rename(patterns) =>
        patterns.map { case (from, to) =>
          val jrule = new Rule()
          jrule.setPattern(from)
          jrule.setResult(to)
          jrule
        }
      case ShadeRule.Zap(patterns) =>
        patterns.map { case pattern =>
          val jrule = new Zap()
          jrule.setPattern(pattern)
          jrule
        }
      case ShadeRule.Keep(patterns) =>
        patterns.map { case pattern =>
          val jrule = new Keep()
          jrule.setPattern(pattern)
          jrule
        }
      case _ => Nil
    }}

    val proc = JJProcessor(jjrules, verbose = level == Level.Debug, true)
    val files = AssemblyUtils.getMappings(dir, Set())

    val entry = new EntryStruct
    files filter (!_._1.isDirectory) foreach { f =>
      entry.data = IO.readBytes(f._1)
      entry.name = f._2
      entry.time = -1
      IO.delete(f._1)
      if (proc.process(entry)) {
        IO.write(dir / entry.name, entry.data)
      }
    }
    val excludes = proc.getExcludes()
    excludes.foreach(exclude => IO.delete(dir / exclude))
  }

}