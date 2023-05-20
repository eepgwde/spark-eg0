package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec

import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.file.FileSystems
import scala.collection.mutable

/**
 * Create the input pipeline and archive.
 *
 */
class Stage0Test extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("Stage0Test")
  val modeller = new UserLDA()
  var df1: Option[DataFrame] = None

  describe("LDA processing") {
    it("pipeline0 - load and simplify") {
      val spark = Session0.instance
      spark should not be null

      logger.info("UserLDA: ${modeller.initial0}")

      val url = "file:///a/l/X-image/cache/data/abcnews-date-text.csv"

      val type0 = "csv"
      val infer_schema = "true"
      val first_row_is_header = "true"
      val delimiter = ","

      val df0 = spark.read.format(type0)
        .option("inferSchema", infer_schema)
        .option("header", first_row_is_header)
        .option("sep", delimiter)
        .load(url)

      df1 = Some(modeller.pipeline0(df0))
      df1 should not be (None)
    }
    it("archive0 - write the stage0 table to Hive") {
      modeller.archive0()
    }
    it("close") {
      Session0.instance.close()
    }
  }
}
