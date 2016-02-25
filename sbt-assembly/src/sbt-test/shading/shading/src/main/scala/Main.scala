import org.apache.commons.io.filefilter.AgeFileFilter

object Main extends App {
  val filter = new AgeFileFilter(0)
  println("hello " + filter.getClass.getName.toString)
}
