package artikus.spark.ctl

import artikus.spark.Session0
import com.typesafe.scalalogging.Logger
import org.scalatest.funspec.AnyFunSpec


/**
 * Run jobs for each of the input pipelines.
 *
 * Remember that array processing is performed by the receiver, so Array("do this") is not the same as
 * Array("do", "this").
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

  describe("Spark close") {
    it("close") {
      Session0.instance.close()
    }
  }
}
