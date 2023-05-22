package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, size}
import org.scalatest.funspec.AnyFunSpec
import org.apache.spark.sql.functions._
import org.apache.spark.ml._

/**
 * Create the input pipeline and archive.
 *
 * The UserLDA class uses 10000 records and there is one test record in there.
 * After CountVectorization, pipeline1, it is possible that the dataframe will be empty.
 *
 * To do any useful analysis, use the whole 1000000 records and increase the vocabulary size.
 * There is a warn message in the logger if the hit rate is very low.
 */
class Stage1Test extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("Stage1Test")
  var modeller: Option[UserLDA] = None
  var df1: Option[DataFrame] = None

  val isStats = false
  var archiving = false

  describe("LDA pre-processing 2") {
    it("load stage0") {
      val session = Session0.instance
      session should not be null

      assert(Session0.instance.catalog.tableExists("stage0"))

      modeller = UserLDA.unserialize()
      if (modeller.isEmpty) {
        if (Session0.instance.catalog.tableExists("stage0")) {
          modeller = Some(new UserLDA())
          modeller.get.stage0 = Some(Session0.instance.sql("select * from stage0"))
          modeller.get.stage = Stage.Raw0
        }
      }
      // consistency checks
      // unserialize can be problematic
      modeller should not be null
      modeller should not be (None)

      // check the pipeline is at the right stage.
      logger.info(s"pipeline0: ${modeller.get.stage}")
      logger.info(s"UserLDA: ${modeller.get.initial0}")
      modeller.get.stage should be(Stage.Raw0)

      modeller.get.stage0 should not be null
      modeller.get.stage0 should not be (None)

      if (archiving)
        modeller.get.stage0 = Some(Session0.instance.sql("select * from stage0"))

      modeller.get.stage0 should not be (None)
    }
    it("pipeline1") {
      modeller should not be (None)

      if (isStats) {
        modeller.get.tokensN = -1
        modeller.get.vocabN = 1000
        modeller.get.minTF = 0.05
      }

      assert(modeller.get.stage0 != null)
      modeller.get.stage0 should not be (None)
      val cnt0 = modeller.get.stage0.get.count()

      df1 = modeller.get.pipeline1(modeller.get.stage0.get)
      df1 should not be (None)

      modeller.get.stage1 should not be (None)
    }
    it("archive1 and serialize") {
      modeller should not be (None)
      // the name of the stage one database and the serialization name should be the same.
      if (archiving) modeller.get.archive1()
      UserLDA.serialize(modeller.get)
    }
  }

  describe("Spark close") {
    it("close") {
      Session0.instance.close()
    }
  }
}
