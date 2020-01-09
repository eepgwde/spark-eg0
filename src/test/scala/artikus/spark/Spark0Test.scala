package artikus.spark
/** This is the Scaladoc for the package. */

import org.scalatest.funspec.AnyFunSpec
import org.scalatest.matchers.should.Matchers

import org.scalatest._
import Assertions._
import Inspectors._

// Same package
// import artikus.spark.GetLog

class GetLogTest extends AnyFunSpec with org.scalatest.Inspectors 
    with org.scalatest.matchers.should.Matchers {

  val xs = List[String]("weaves", "again", "again2")

   describe ("Log 1 activation message") {                        // <4>
    it ("once") {
      forAll (xs) { x => GetLog(x) }
    }
  }

  describe ("Log no activation messages") {                        // <4>
    it ("once") {
      forAll (xs) { x => GetLog(x) }
      val l0 = GetLog("weaves")
      l0.warn("warn")
      l0.debug("debug")
      l0.info("info")
      l0.error("error")
      l0.trace("trace")
    }
  }

  describe ("Log new activation") {                        // <4>
    it ("once") {
      val l0 = GetLog()
      l0.warn("warn")
      l0.debug("debug")
      l0.info("info")
      l0.error("error")
      l0.trace("trace")
    }
  }

}
