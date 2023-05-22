package artikus.spark.ctl

import artikus.spark.{Session0, Stage, UserLDA}
import com.typesafe.scalalogging.Logger

import scala.math.random


trait Staging {
  def args: Array[String]

  def run: Unit
}

class Logged {
  val logger: Logger = Logger("LDA-staging")
}

case class Fail0(args: Array[String]) extends Logged with Staging {
  def run(): Unit = {
    System.err.println("no action stated")
    logger.error(s"Fail0: no action stated: ${args}")
  }

}

case class SparkState(args: Array[String]) extends Logged with Staging {
  val spark = Session0.instance

  def run(): Unit = {
    logger.info(s"SparkState: ${args}")
    spark.conf.getAll foreach (x => logger.info(x._1 + " --> " + x._2))
  }

}

case class DbClean(args: Array[String]) extends Logged with Staging {
  val spark = Session0.instance

  def run(): Unit = {
    logger.info(s"DbClean: ${args}")
    val unused = Session0.unusedTables()
    logger.info(s"DbClean: unused-tables: ${unused}")

    if (!unused.isEmpty && isDelete(args)) {
      logger.info("DbClean: will delete")
      Session0.dropTables(unused)
    }
  }

  protected def isDelete(args: Array[String]): Boolean = {
    if (args.length <= 0) return false
    logger.info(s"DbClean: argument ${args(0)}")
    if (args(0).toLowerCase() != "delete") return false
    true
  }
}

case class Pi(val args: Array[String]) extends Logged with Staging {
  val slices = if (args.length > 0) args(0).toInt else 2
  val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow

  def run() = {
    val spark = Session0.instance
    val count = spark.sparkContext.parallelize(1 until n, slices).map { i =>
      val x = random * 2 - 1
      val y = random * 2 - 1
      if (x * x + y * y <= 1) 1 else 0
    }.reduce(_ + _)
    logger.info(s"Pi is roughly ${4.0 * count / (n - 1)}")
  }
}

case class Raw0(args: Array[String]) extends Logged with Staging {
  val spark = Session0.instance
  val url = "file:///a/l/X-image/cache/data/abcnews-date-text.csv"

  val type0 = "csv"
  val infer_schema = "true"
  val first_row_is_header = "true"
  val delimiter = ","

  def run() = {
    val df0 = spark.read.format(type0)
      .option("inferSchema", infer_schema)
      .option("header", first_row_is_header)
      .option("sep", delimiter)
      .load(url)

    val modeller = new UserLDA()

    modeller.pipeline0(df0)
    require(modeller.stage0.isDefined)
    UserLDA.serialize(modeller)
  }
}

case class CV1(args: Array[String]) extends Logged with Staging {
  var modeller: Option[UserLDA] = None

  def run() = {
    modeller = UserLDA.unserialize()
    // consistency checks
    // unserialize can be problematic
    require(modeller != null)
    require(modeller.isDefined)

    // check the pipeline is at the right stage.
    logger.info(s"pipeline0: ${modeller.get.stage}")
    logger.info(s"UserLDA: ${modeller.get.initial0}")
    require(modeller.get.stage == Stage.Raw0)

    require(modeller.get.stage0 != null)
    require(modeller.get.stage0.isDefined)
    modeller.get.pipeline1(modeller.get.stage0.get)
    UserLDA.serialize(modeller.get)
  }
}

case class LDA2(args: Array[String]) extends Logged with Staging {
  var modeller: Option[UserLDA] = None

  def run() = {
    modeller = UserLDA.unserialize()
    // consistency checks
    // unserialize can be problematic
    require(modeller != null)
    require(modeller.isDefined)

    // check the pipeline is at the right stage.
    logger.info(s"pipeline0: ${modeller.get.stage}")
    logger.info(s"UserLDA: ${modeller.get.initial0}")
    require(modeller.get.stage == Stage.CV1)

    require(modeller.get.stage1 != null)
    require(modeller.get.stage1.isDefined)
    modeller.get.pipeline1(modeller.get.stage1.get)
    UserLDA.serialize(modeller.get)
  }
}

object LDA {

  // Unable to run two jobs consecutively in LDATest, because stop is called
  // Running consecutive jobs only seems to work reliably in local mode.
  var testing = true

  def main(args: Array[String]): Unit = {
    // args.foreach(x => System.err.println(s"main: args: ${x}"))
    val stg0 = if (args.length >= 1) make(args) else Fail0(Array[String](""))
    stg0.run
    stg0 match {
      case Fail0(args: Array[String]) => ()
      case _ => {
        // System.err.println(s":: completed ${stg0}")
        if (!testing) Session0.stop()
      }
    }
  }

  def make(args: Array[String]): Staging = {
    // args.foreach(x => System.err.println(s"make: args: ${x}"))

    val x0 = args(0).toLowerCase() match {
      case "state" => SparkState(args.tail)
      case "pi" => Pi(args.tail)
      case "stage0" => Raw0(args.tail)
      case "raw0" => Raw0(args.tail)
      case "cv1" => CV1(args.tail)
      case "count-vectors" => CV1(args.tail)
      case "lda2" => LDA2(args.tail)
      case "dbclean" => DbClean(args.tail)
      case _ => Fail0(args.tail)
    }
    x0
  }

}
