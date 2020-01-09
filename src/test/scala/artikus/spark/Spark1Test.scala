package artikus.spark
/** This is the Scaladoc for the package. */

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import org.scalatest._
import Assertions._
import Inspectors._

// Same package
// import artikus.spark.GetLog

class GetLog1Test extends AnyFunSpec with org.scalatest.Inspectors 
    with org.scalatest.matchers.should.Matchers {

  describe ("Default invocation") {                        // <4>
    it ("using identity") {
      val l0 = GetLog()
      l0.warn("warn")
      l0.debug("debug")
      l0.info("info")
      l0.error("error")
      l0.trace("trace")
    }
  }

}
