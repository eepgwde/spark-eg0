package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.FileSystems
import scala.collection.mutable

/**
 * Write a simple table to Hive.
 *
 * This can be checked by HiveRTest.
 */
class HiveWTest extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("HiveWTest")

  var df1: Option[DataFrame] = None

  describe ("Hive Test") {
    it("testdata") {
      val spark = Session0.instance

      val columns = Seq("language","users_count")
      val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
      val rdd = spark.sparkContext.parallelize(data)
      df1 = Some(spark.createDataFrame(rdd))

    }
    it("write to Hive") {
      val tname = "xusers"
      df1.get.createTempView(s"t${tname}")

      df1.get.sqlContext.sql(s"drop table if exists ${tname}")
      df1.get.sqlContext.sql(s"create table ${tname} AS select * from t${tname}")
      df1.get.sqlContext.sql(s"drop table if exists t${tname}")

    }
    it("check tables") {
      logger.info(s"table-names: ${df1.get.sqlContext.tableNames()}")
    }
    it("close") {
      Session0.instance.close()
    }
  }
}
