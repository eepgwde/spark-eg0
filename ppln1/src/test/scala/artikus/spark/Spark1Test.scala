package artikus.spark
/** This is the Scaladoc for the package. */

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers
import com.typesafe.scalalogging.Logger

import artikus.spark.UserLDA

import org.scalatest._
import Assertions._
import Inspectors._
class GetLog1Test extends AnyFunSpec with org.scalatest.Inspectors 
    with org.scalatest.matchers.should.Matchers {

  val l0 = Logger("name")

  describe ("Instantiation") {                        // <4>
    it("properties") {
      val props = Session0.props()
      l0.info(props.toString())
    }
    it("configure") {
      val conf0 = Session0.configure()
      l0.info(conf0.toDebugString)
    }
    it("session") {
      val session = Session0.session
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

      val modeller = new UserLDA()
      val df1 = modeller.pipeline0(df0)
    }
  }
}
