package artikus.spark
/** This is the Scaladoc for the package. */

import java.io.File
import org.apache.commons.io.FileUtils
  // import org.apache.commons.io.filefilter.WildcardFileFilter

class Identity(val version:String = "0.4")

/** Utility methods for working with Spark. */
object U {

  val id = new Identity

  def identity = id

  /** Get the URLs of an object's class loader's paths. */
  def classes(x: Object): Seq[String] = {
    val cl = x.getClass().getClassLoader()
    return cl.asInstanceOf[java.net.URLClassLoader].getURLs.map(x => x.toString())
  }

  /** Given a directory's path as a string, give a list of files in it. */
  def flist(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      d.listFiles.filter(_.isFile).toList
    } else {
      List[File]()
    }
  }

  /** Print the class of an object. */
  def printClass(x: Any) { println(x.getClass) }

  /** Create a URI from a file and a directory. */
  def local0(f0: String, pwd: String) : java.net.URI = {
    // temp
    val f1 = new java.io.File(pwd, f0.toString());
    return new java.net.URI("file", f1.toString(), null) 
  }

  def local1(pwd: String) = (fname: String) => this.local0(fname, (new File(pwd)).getCanonicalPath().toString()) ;

  def rmdir(ddir: String) { FileUtils.deleteDirectory(new File(ddir)) }

  def alert(msg: String) { println(msg) }
}

/**
  * Of course all that was nothing to do with CSV files.
  * What we had was a file finder, which could be implemented with a factory object.
  * 
  * And we can use an underlying Java Stream
  *  I'm using Scala 2.12 but in Scala 2.13
  * Scala has renamed its Stream to LazyList
  * and this provide implicit converters
  * import scala.jdk.StreamConverters._
  * https://stackoverflow.com/questions/38339440/how-to-convert-a-java-stream-to-a-scala-stream
  */

object Filing {
  import java.nio.file.{Files, Paths, Path}
  import java.io.{File, IOException}

  import scala.collection.JavaConverters._

  /** Cast to Path. */
  def toPath(x: Any) : Path = x match {
    case s:String => (new File(s)).toPath
    case f:File => f.toPath
    case p:Path => p
  }

  /** Cast to file. */
  def toFile(x: Any) : File = x match {
    case s:String => new File(s)
    case f:File => f
    case p:Path => p.toFile
  }

  /** A directory listing as a Scala Stream/LazyList. */
  @throws(classOf[IOException])
  def apply(x:Any)(glob: String = "*") : Stream[Path] =
    Files.newDirectoryStream(toPath(x), glob).iterator().asScala.toStream

  val readables : (Stream[java.nio.file.Path] => Stream[java.nio.file.Path]) = _.map {
    _.toFile } filter { _.canRead } filter { _.length > 0 } map { _.toPath }

}

// *** Logging utility

/** A logger singleton.
  *
  * GetLog("weaves") will create a logger using ch.qos.logback
  * A call to this will give a logfile status.
  * It's a singleton.
  */

object GetLog {
  /* limit scope of imports */
  import com.typesafe.scalalogging._

  import org.slf4j.Logger
  import org.slf4j.LoggerFactory

  def make(s:Any, mark0:String) : Option[Logger] = {
    import ch.qos.logback.classic.LoggerContext
    import ch.qos.logback.core.util.StatusPrinter

    // this has stopped working
    try {
      val lc = LoggerFactory.getILoggerFactory().asInstanceOf[ch.qos.logback.classic.LoggerContext]
      StatusPrinter.print(lc)
    } catch {
      case cc: java.lang.ClassCastException => ()
    }

    val logger = s match {
      case s:String => LoggerFactory.getLogger(s)
      case c:Class[_] => LoggerFactory.getLogger(c)
    }
    logger.debug(s"--- $mark0 ---")
    return Some(logger)
  }

  var impl : Option[Logger] = None

  def apply(n:Any) : Logger = {
    this.impl = n match {
      case n:String => this.impl.orElse(this.make(n,n))
      case n:Class[_] => this.impl.orElse(this.make(n, n.getClass.getName))
    }
    return this.impl.get
  }

  def apply() : Logger = this.apply(U.identity.getClass)
}


// val l0 = GetLog("weaves")


// * Postamble

// Local Variables:
// mode:scala
// comment-column:50
// comment-start: "// "
// comment-end: ""
// outline-regexp: "// [\*\f]+"
// eval: (xscala-minor-mode)
// eval: (auto-fill-mode)
// eval: (outline-minor-mode)
// fill-column: 85
// End:
