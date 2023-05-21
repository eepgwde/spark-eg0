package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec

/**
 * Matching stage1 with serializations
 *
 * Check that the Hive tables can be accessed.
 */
class HiveR1Test extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("HiveR1Test")

  var df1: Option[DataFrame] = None

  var mr0 : Option[String] = None

  val tname = "stage1"

  var prfx0 : Option[Path] = None

  describe("Hive and Serialized names") {
    it("List tables - most recent prefix") {
      val spark = Session0.instance

      val tlist = spark.catalog.listTables().select("name")
        .collect().map(_.getString(0)).filter(_.startsWith(s"${tname}_"))
        .map(_.replace(s"${tname}_", ""))
        .map(_.toUpperCase())

      mr0 = Some(tlist.sorted.reverse.head)

      logger.info(s"stage1: mr0: ${mr0.get}")
    }
    it("make Path") {
      val spark = Session0.instance

      logger.info(s"stage1: mr0: ${mr0.get}")
      prfx0 = Some(UserLDA.mkPrefix(mr0.get))
      logger.info(s"stage1: prfx0: ${prfx0.get}")
    }
    it("check Path") {
      prfx0 should not be (None)
      logger.info(s"stage1: mr0: ${prfx0.get}")

      val spark = Session0.instance

      val status0 = Session0.logFileStatus(prfx0.get)
      if (!status0.isEmpty)
        logger.info(s"stage1: status: ${status0}")

    }
    it("unserialize - last") {
      val ulda0 = UserLDA.unserialize()
      ulda0 should not be (None)
      logger.info(s"stage1: initial: ${ulda0.get.initial0}; vocab-size: ${ulda0.get.vocab.get.size}")
    }
    it("unserialize - named") {
      val ulda0 = UserLDA.unserialize(prfx0)
      ulda0 should not be (None)
      logger.info(s"stage1: initial: ${ulda0.get.initial0}; vocab-size: ${ulda0.get.vocab.get.size}")
    }
  }

  describe("Spark close") {
    it("close") {
        Session0.instance.close()
    }
  }

}
