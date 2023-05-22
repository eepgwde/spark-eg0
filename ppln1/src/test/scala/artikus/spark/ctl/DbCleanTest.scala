package artikus.spark.ctl

import artikus.spark.Session0
import artikus.spark.artikus.spark.ctl.LDA
import com.typesafe.scalalogging.Logger
import org.scalatest.funspec.AnyFunSpec


/**
 * Create the input pipeline and archive.
 *
 */
class DbCleanTest extends AnyFunSpec with org.scalatest.Inspectors
  with org.scalatest.matchers.should.Matchers {

  var logger: Logger = Logger("DbCleanTest")
  // val session = Session0.instance

  describe("") {
    it("testing variable") {
      LDA.testing = true
    }
  }
  describe("database-clean") {
    it("enquire") {
      LDA.main(Array("dbclean"))
    }
    it("remove") {
      LDA.main(Array("dbclean", "delete"))
    }
  }

  describe("Spark close") {
    it("close") {
      Session0.instance.close()
    }
  }
}
