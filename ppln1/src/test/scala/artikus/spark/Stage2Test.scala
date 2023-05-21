package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec

import scala.collection.mutable

/**
 * Create the input pipeline and archive.
 *
 */
class Stage2Test extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("Stage2Test")
  val modeller = new UserLDA()
  var df2: Option[DataFrame] = None
  var words: Option[List[List[Scores1]]] = None
  var desc: Option[List[mutable.WrappedArray[(String, Double)]]] = None

  val isStats = false

  describe("LDA processing") {
    it("unarchive1 - load stage1") {
      val session = Session0.instance
      session should not be null

      logger.info("UserLDA: ${modeller.initial0}")

      modeller.stage1 = None
      assert(Session0.instance.catalog.tableExists("stage1"))
      modeller.unarchive1()
    }
    it("pipeline2 - LDA fit - topics with words then scores") {
      modeller.stage1 should not be (None)

      if (isStats) {
        modeller.itersN = 100
        modeller.topicsN = 10
      }

      modeller.pipeline2(modeller.stage1.get)

      modeller.topics should not be (None)
    }
    it("archive2 - write down transformed") {
      modeller.archive2()
    }
    it("pipeline3 - topics") {
      if (isStats) {
        modeller.vocabN = 500
        modeller.minTF = 3.0
      }

      modeller.vocab should not be (None)

      words = modeller.pipeline3(modeller.topics.get)
    }
    it("display - quality") {
      words should not be (None)
      modeller.display()

      val tr0 = modeller.quality0()
      logger.info(s"quality: ${tr0}")
    }
    it("serialize") {
      words should not be (None)
      UserLDA.serialize(modeller)
    }
    it("close") {
      Session0.instance.close()
    }
  }
}
