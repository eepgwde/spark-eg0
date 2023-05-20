package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec

/**
 * Create the input pipeline and archive.
 *
 */
class Stage1Test extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("Stage1Test")
  val modeller = new UserLDA()
  var df1: Option[DataFrame] = None

  val isStats = false

  describe("LDA processing") {
    it("pipeline1 - load stage0") {
      val session = Session0.instance
      session should not be null

      logger.info("UserLDA: ${modeller.initial0}")

      modeller.stage0 = None
      assert(Session0.instance.catalog.tableExists("stage0"))
      modeller.stage0 = Some(Session0.instance.sql("select * from stage0"))
    }
    it("archive0 - run pipeline1") {
      modeller.stage0 should not be (None)

      modeller.tokensN = if (isStats) 100 else 5000

      df1 = Some(modeller.pipeline1(modeller.stage0.get))
      df1 should not be (None)

      modeller.stage1 should not be (None)
    }
    it("archive0 - write the stage1 table to Hive") {
      modeller.archive1()
    }
    it("close") {
      Session0.instance.close()
    }
  }
}
