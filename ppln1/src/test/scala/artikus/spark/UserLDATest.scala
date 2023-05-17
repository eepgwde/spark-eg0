package artikus.spark
/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable
class UserLDATest extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val l0: Logger = Logger("UserLDATest")

  val modeller = new UserLDA()
  var session: Option[SparkSession] = None

  // case class Table1(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])


  var df1: Option[DataFrame] = None
  var df2: Option[DataFrame] = None
  var rdd3: Option[RDD[Row]] = None
  var desc: Option[List[mutable.WrappedArray[(String, Double)]]] = None

  describe ("LDA processing") {
    it("properties") {
      val props = Session0.props()
      l0.info(props.toString())
      props should not be null
      assert(props.size() > 0)
    }
    it("configuration") {
      val conf0 = Session0.configure()
      conf0 should not be null
      l0.info(conf0.toDebugString)
    }
    it("pipeline0 - load and simplify") {
      session = Some(Session0.session)
      session should not be null

      l0.info(session.get.version)

      val url = "file:///a/l/X-image/cache/data/abcnews-date-text.csv"

      val type0 = "csv"
      val infer_schema = "true"
      val first_row_is_header = "true"
      val delimiter = ","

      val df0 = session.get.read.format(type0)
        .option("inferSchema", infer_schema)
        .option("header", first_row_is_header)
        .option("sep", delimiter)
        .load(url)

      df1 = Some(modeller.pipeline0(df0))
      df1 should not be (None)
    }
    it("pipeline1 - count vectorization") {
      df1 should not be (None)
      df2 = Some(modeller.pipeline1(df1.get))
      df2 should not be (None)
    }
    it("pipeline2 - LDA fit") {
      df2 should not be (None)
      rdd3 = Some(modeller.pipeline2(df2.get))
      rdd3 should not be (None)
    }
    it("pipeline3 - map vocabulary to topics") {
      rdd3 should not be (None)
      desc = Some(modeller.pipeline3(rdd3.get, session.get))
      // List[mutable.WrappedArray[(String, Double)]]
      desc should not be (None)
    }
    it("display - topics") {
      desc should not be (None)
      modeller.display(session.get)
    }
  }
}
