package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{size,col}
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

  val isStats = true

  describe("LDA processing") {
    it("load stage0") {
      val session = Session0.instance
      session should not be null

      logger.info("UserLDA: ${modeller.initial0}")

      modeller.stage0 = None
      assert(Session0.instance.catalog.tableExists("stage0"))
      modeller.stage0 = Some(Session0.instance.sql("select * from stage0"))
    }
    it("pipeline1") {
      modeller.stage0 should not be (None)

      if (isStats) {
        modeller.tokensN = -1
        modeller.vocabN = 1000
        modeller.minTF = 0.05
      }

      df1 = modeller.pipeline1(modeller.stage0.get)

      val vdf1 = modeller.vdf1.get

      vdf1.printSchema()
      vdf1.show(truncate=false)

      // this test fails if any of the data is null.
      val chk0 = vdf1
        .filter( org.apache.spark.sql.functions.col("features.indices"))
        .collect().size
      logger.info(s"pipeline1: chk0: ${chk0}")
      assert(0 > 1)

      df1 should not be (None)

      modeller.stage1 should not be (None)
    }
    it("archive1") {
      modeller.archive1()
    }
    it("close") {
      Session0.instance.close()
    }
  }
}
