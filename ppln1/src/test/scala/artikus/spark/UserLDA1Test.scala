package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable

/**
 * Extra stages.
 *
 * Archive to disk.
 */
class UserLDA1Test extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("UserLDATest")

  val modeller = new UserLDA()

  val statTest = false

  // case class Table1(id: Int, indices: mutable.WrappedArray[Int], scores: mutable.WrappedArray[Double])

  var df1: Option[DataFrame] = None
  var df2: Option[DataFrame] = None
  var topics: Option[List[Scores0]] = None
  var words: Option[List[List[Scores1]]] = None
  var desc: Option[List[mutable.WrappedArray[(String, Double)]]] = None

  describe ("Archive") {
    it("pipeline0 - load and simplify") {
      modeller.archive0(reload=true)
      modeller.stage0 should not be (None)
    }
    it("pipeline1 - count vectorization") {
      // set some parameters here
      // this matches the settings in scala-lda and usually hits 3 in just over 10 minutes
      // 4 cores and 4 g
      if (statTest) {
        modeller.itersN = 100
        modeller.tokensN = 100000
        modeller.vocabN = 500
        modeller.minTF = 3.0
      }

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
    it("quality - messages classified") {
      val tr0 = modeller.quality0()
      logger.info(s"quality: ${tr0}")
      // only if we are using a statistical test configuration should we expect any topics.
      if (statTest) {
        assert(tr0.length > 0)
      } else {
        assert(!statTest)
      }
    }
  }
}
