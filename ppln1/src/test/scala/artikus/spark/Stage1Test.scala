package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{size,col}
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
  val modeller = new UserLDA()
  var df1: Option[DataFrame] = None

  val isStats = false

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
      df1 should not be (None)

      modeller.stage1 should not be (None)

    }
    it("archive1 and serialize") {
      // the name of the stage one database and the serialization name should be the same.
      modeller.archive1()
      UserLDA.serialize(modeller)
    }
    it("close") {
      Session0.instance.close()
    }
  }
}
