package artikus.spark

/** This is the Scaladoc for the package. */

import com.typesafe.scalalogging.Logger
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.scalatest.funspec.AnyFunSpec

/**
 * Hive table tests.
 *
 * Check that the Hive tables can be accessed.
 */
class HiveRTest extends AnyFunSpec with org.scalatest.Inspectors
    with org.scalatest.matchers.should.Matchers {

  val logger: Logger = Logger("HiveRTest")

  var df1: Option[DataFrame] = None

  describe("access system configuration") {
    it("initial timestamp") {
      logger.info(s"timestamp as string: ${DateID.initial0}")
    }
    it("hadoop configuration: URL") {
      logger.info(s"configuration: URL: ${Session0.configurations()}")
    }
    it("hadoop configuration: configuration") {
      logger.info(s"configuration: fs: fails: ${Session0.hadoop0.getAllPropertiesByTag("HDFS")}")
      logger.info(s"configuration: fs: ${Session0.hadoop0.get("fs.defaultFS")}")
      logger.info(s"configuration: username: environment is unset: ${scala.util.Properties.envOrNone("HADOOP_USER_NAME")}")
      logger.info(s"session: username: properties file is set: ${Session0.getUsername()}")
    }
  }
  describe("serialization") {
    it("UserLDA: filesystem") {
      // we don't use the home directory, because we work as hadoop.
      logger.info(s"Home-Directory: ${Session0.fs.getHomeDirectory()}")
      logger.info(s"userBase: ${UserLDA.userBase}")

      val p0 = Path.mergePaths(UserLDA.userBase, new Path("/UserLDA.ser"))
      logger.info(s"mergePaths: ${p0}")

      val lda = new UserLDA()
      val r0 = UserLDA.serialize(lda)
    }
    it("hdfs directory") {
      logger.info(s"userBase: ${UserLDA.userBase}")
      val g0 = Session0.fs.globStatus(new Path(s"${UserLDA.userBase}/*T*Z"))
      logger.info(s"g0: ${g0}")
      val ulda = UserLDA.unserialize()
      ulda should not be (None)
      logger.info(s"LDA: initial0: ${ulda.get.initial0}")
    }

  }

  describe("Hive Test") {
    it("check tables") {
      val spark = Session0.instance
      logger.info(s"table-names: ${spark.sqlContext.tableNames()}")
    }
    it("read from Hive") {
      val spark = Session0.instance

      val df0 = spark.sql("select * from finalTable")
      df1 = Some(df0)
    }
    it("display table") {
      df1.get.show()
    }
  }

  var mr0 : Option[String] = None

  describe("Hive and Serialized names") {
    it("List tables") {
      val spark = Session0.instance

      val tname = "stage1"
      val tlist = spark.catalog.listTables().select("name")
        .collect().map(_.getString(0)).filter(_.startsWith(s"${tname}_"))

      mr0 = Some(tlist.sorted.reverse.head)

      logger.info("stage1: mr0: ${mr0}")
    }
    it("List serializations") {
      val spark = Session0.instance
    }
    it("display table") {
      df1.get.show()
    }
  }

  describe("Spark close") {
    it("close") {
        Session0.instance.close()
    }
  }

}
