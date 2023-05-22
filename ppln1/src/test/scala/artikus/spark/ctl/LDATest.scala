package artikus.spark.ctl

import artikus.spark.artikus.spark.ctl.LDA
import com.typesafe.scalalogging.Logger
import org.scalatest.funspec.AnyFunSpec


/**
 * Create the input pipeline and archive.
 *
 */
class LDATest extends AnyFunSpec with org.scalatest.Inspectors
  with org.scalatest.matchers.should.Matchers {

  var logger: Logger = Logger("LDATest")
  // val session = Session0.instance

  describe("LDA preset") {
    it("testing variable") {
      LDA.testing = true
    }
  }
  describe("LDA processing - launchers") {
    it("state") {
      LDA.main(Array("state"))
    }
    it("pi") {
      LDA.main(Array("pi"))
    }
    it("Raw0") {
      LDA.main(Array("raw0"))
    }
    it("CV1") {
      LDA.main(Array("cv1"))
    }
    it("LDA2") {
      LDA.main(Array("lda2"))
    }
  }
}
