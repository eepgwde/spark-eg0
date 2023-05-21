package artikus.spark
/** This is the Scaladoc for the package. */

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.FileNotFoundException
import java.net.URL
import java.util.{Calendar, Properties}
import scala.collection.mutable

import java.text.ParsePosition
import java.text.SimpleDateFormat
import java.util.Date
import java.util.TimeZone

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import com.typesafe.scalalogging.Logger

class Identity(val version:String = "0.4")

class DateID {
  // ISO 8601 BASIC is used as a signature.

  val ISO_8601BASIC_DATE_PATTERN = "yyyyMMdd'T'HHmmss'Z'"

  def init0() = {
    val dateFormat = new SimpleDateFormat(ISO_8601BASIC_DATE_PATTERN)
    dateFormat.setLenient(false)
    dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"))
    dateFormat
  }

  val dateFormat = init0()

  def isIsoTimestamp(s: String): Boolean = s.matches("\\d{8}T\\d{6}Z")

  def parseIsoDateTime(s: String): Date = {
    val result = dateFormat.parse(s, new ParsePosition(0))
    result
  }

  def getTimestamp() : String = {
    val calendar = Calendar.getInstance()
    val time0 = calendar.getTime()
    dateFormat.format(time0)
  }
}

object DateID {
  protected val instance = new DateID()
  val initial0 = instance.getTimestamp()
}

/**
 * Utility methods for working with Spark.
 *
 * This provides a method to start-up a Spark session. It cannot be used with a YARN cluster. You can only submit
 * jobs to a YARN cluster. This one, starts locally.
 */
object Session0 {
  val logger: Logger = Logger("Session0")

  val id = new Identity

  def identity = id

  /**
   * This provides the variables for the SPARK and HADOOP configuration directory variables
   *
   * spark0.properties is in test/resources and is a soft link to ~hadoop/x-hadoop.env
   * @return
   */
  def props () : Properties = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/spark0.properties"))
    logger.info("logging in Session0")
    properties
  }

  val spark0: Properties = props()

  def getUsername() = {
    val user = System.getProperty("user.name").asInstanceOf[String]
    spark0.getOrDefault("HADOOP_USER_NAME", user).asInstanceOf[String]
  }

  def configurations() = {
    val hconf = spark0.getOrDefault("HADOOP_CONF_DIR", scala.util.Properties.envOrNone("HADOOP_CONF_DIR"))
    val cfiles = List[String]("core-site.xml", "hdfs-site.xml", "yarn-site.xml")
    cfiles.map(x => s"file://${hconf}/${x}").map(new URL(_))
  }

  def hadoopConfig() = {
    val hadoop0 = new Configuration(false)
    configurations().foreach(hadoop0.addResource(_))
    hadoop0.setQuietMode(false)

    val fs = FileSystem.newInstance(hadoop0)

    (hadoop0, fs)
  }

  val (hadoop0, fs) = hadoopConfig()

  def logFileStatus(p0: Path): Option[FileStatus] = {
    var status0: Option[FileStatus] = None
    try {
      status0 = Some(Session0.fs.getFileStatus(p0))
    } catch {
      case e0: FileNotFoundException => logger.info("directory not present: " + p0);
      case _: Throwable => throw new IllegalStateException(s"could not create directory: ${p0}")
    }
    if (!status0.isEmpty)
      logger.info(s"directory ${p0} isDirectory: ${status0.get.isDirectory}; time: ${status0.get.getModificationTime}")
    else logger.info(s"file not found: ${p0}")
    status0
  }

  def configure(): SparkConf = {
    val conf = new SparkConf()

    // YARN - can't be invoked from here.
    // conf.setMaster("yarn-client")
    // conf.setMaster("spark://k1:8088")
    conf.setAppName("spark0")

    val sparkHome = spark0.getProperty("SPARK_HOME", "/misc/share/1/spark")

    conf.setSparkHome(sparkHome)

    conf.set("spark.driver.cores", "4")
    conf.set("spark.executor.cores", "4")
    conf.set("spark.executor.memory", "4g")
    conf.set("spark.executor.instances", "4")
    conf.set("spark.hadoop.fs.permissions.umask-mode", "002")
    conf.set("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.2")
    conf.set("spark.sql.catalogImplementation", "hive")
    conf.set("spark.sql.warehouse.dir", "file:///home/hadoop/data/hive")
    conf.set("spark.driver.extraClassPath", ":/misc/build/0/classes/:/usr/share/java/postgresql.jar")

    conf
  }

  val config0: SparkConf = configure()

  var session0 : Option[SparkSession] = None

  case class Table1(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])

  /**
   * A singleton for the SparkSession.
   *
   * @return the SparkSession
   */
  def instance: SparkSession = {
    if (session0.isEmpty) {
      val s1 = SparkSession.builder().config(config0).master("local[*]").getOrCreate()
      session0 = Some(s1)
    }

    session0.get
  }

  def stop(): Unit = {
    session0.get.sparkContext.stop()
  }

  def close(): Unit = stop()
}

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
