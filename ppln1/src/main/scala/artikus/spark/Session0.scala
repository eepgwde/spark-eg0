package artikus.spark
/** This is the Scaladoc for the package. */

import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.ExceptionFailure
import org.apache.spark.SparkConf
import org.apache.spark.sql.types._

import java.util.Properties
import scala.collection.mutable

class Identity(val version:String = "0.4")

/**
 * Utility methods for working with Spark.
 *
 * This provides a method to start-up a Spark session. It cannot be used with a YARN cluster. You can only submit
 * jobs to a YARN cluster. This one, starts locally.
 */
object Session0 {
  val id = new Identity
  def identity = id

  def props () : Properties = {
    val properties = new Properties
    properties.load(this.getClass.getResourceAsStream("/spark0.properties"))
    return properties
  }

  val spark0: Properties = props()

  def configure(): SparkConf = {
    val conf = new SparkConf()

    // launcher.master = "yarn"
    // launcher.conf.spark.app.name = "spark-lda"
    // launcher.conf.spark.executor.cores = 8
    // launcher.num_executors = 4
    // launcher.executor_cores = 4
    // launcher.driver_memory = '4g'

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
    conf.set("spark.sql.warehouse.dir", "file:///home/hadoop/data/hive")
    conf.set("spark.sql.catalogImplementation", "hive")
    conf.set("spark.hadoop.fs.permissions.umask-mode", "002")
    conf.set("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:4.4.2")

    conf.set("spark.driver.extraClassPath", ":/misc/build/0/classes/:/usr/share/java/postgresql.jar")

    return conf
  }

  val config0: SparkConf = configure()

  var session0 : Option[SparkSession] = None

  case class Table1(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])

  def instance: SparkSession = {
    val s1 = SparkSession.builder().config(config0).master("local[*]").getOrCreate()
    import s1.implicits._
    session0 = Some(s1)
    session0.get
  }
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
