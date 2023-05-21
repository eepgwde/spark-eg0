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

  var logger: Logger = Logger("Stage2Test")
  val session = Session0.instance

  val isStats = false

  var modeller: Option[UserLDA] = None
  var df2: Option[DataFrame] = None
  var words: Option[List[List[Scores1]]] = None
  var desc: Option[List[mutable.WrappedArray[(String, Double)]]] = None

  describe("LDA processing") {
    it("unserialize - get last") {
      modeller = UserLDA.unserialize()
      modeller should not be null

      logger.info("UserLDA: ${modeller.get.initial0}")
    }
    it("unarchive1 - load stage1") {
      modeller should not be null

      assert(Session0.instance.catalog.tableExists("stage1"))
      modeller.get.unarchive1()
      modeller.get.stage1 should not be (None)
    }
    it("pipeline2 - LDA fit - topics with words then scores") {
      modeller.get.stage1 should not be (None)

      if (isStats) {
        modeller.get.itersN = 100
        modeller.get.topicsN = 10
      }

      modeller.get.pipeline2(modeller.get.stage1.get)

      modeller.get.topics should not be (None)
    }
    it("archive2 - transformed") {
      modeller.get.archive2()
    }
    it("pipeline3 - topics") {
      if (isStats) {
        modeller.get.vocabN = 500
        modeller.get.minTF = 3.0
      }

      modeller.get.vocab should not be (None)

      words = modeller.get.pipeline3(modeller.get.topics.get)
    }
    it("display - quality") {
      words should not be (None)
      modeller.get.display()

      val tr0 = modeller.get.quality0()
      logger.info(s"quality: ${tr0}")
    }
    it("serialize") {
      words should not be (None)
      UserLDA.serialize(modeller.get)
    }
  }
  describe("LDA processing") {
    it("close") {
      Session0.instance.close()
    }
  }
}

