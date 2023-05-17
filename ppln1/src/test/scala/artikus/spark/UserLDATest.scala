package artikus.spark
/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row, SparkSession}
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable
class UserLDATest extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val l0: Logger = Logger("UserLDATest")
  val logger = l0

  val modeller = new UserLDA()

  // case class Table1(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])

  var df1: Option[DataFrame] = None
  var df2: Option[DataFrame] = None
  var topics: Option[List[Scores0]] = None
  var words: Option[List[List[Scores1]]] = None
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
      val session = Session0.instance
      session should not be null

      l0.info(session.version)

      val url = "file:///a/l/X-image/cache/data/abcnews-date-text.csv"

      val type0 = "csv"
      val infer_schema = "true"
      val first_row_is_header = "true"
      val delimiter = ","

      val df0 = session.read.format(type0)
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
    it("pipeline2 - LDA fit - topics with words then scores") {
      df2 should not be (None)
      topics = Some(modeller.pipeline2(df2.get))
      topics should not be (None)
    }
    it("pipeline3 - topic each word with score") {
      topics should not be (None)

      words = modeller.pipeline3(topics.get)
      words should not be (None)
    }
    it("display - topics") {
      words should not be (None)
      modeller.display()
    }
  }
}